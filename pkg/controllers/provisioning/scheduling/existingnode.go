/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduling

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/scheduling"
	"github.com/aws/karpenter-core/pkg/utils/resources"
)

type ExistingMachine struct {
	Pods          []*v1.Pod
	Node          *v1.Node
	requests      v1.ResourceList
	topology      *Topology
	requirements  scheduling.Requirements
	available     v1.ResourceList
	taints        []v1.Taint
	hostPortUsage *scheduling.HostPortUsage
	volumeUsage   *scheduling.VolumeLimits
	volumeLimits  scheduling.VolumeCount
}

func NewExistingMachine(n *state.Node, topology *Topology, startupTaints []v1.Taint, daemonResources v1.ResourceList) *ExistingMachine {
	// The state node passed in here must be a deep copy from cluster state as we modify it
	// the remaining daemonResources to schedule are the total daemonResources minus what has already scheduled
	remainingDaemonResources := resources.Subtract(daemonResources, n.DaemonSetRequested)
	// If unexpected daemonset pods schedule to the node due to labels appearing on the node which cause the
	// DS to be able to schedule, we need to ensure that we don't let our remainingDaemonResources go negative as
	// it will cause us to mis-calculate the amount of remaining resources
	for k, v := range remainingDaemonResources {
		if v.AsApproximateFloat64() < 0 {
			v.Set(0)
			remainingDaemonResources[k] = v
		}
	}
	node := &ExistingMachine{
		Node:          n.Node,
		available:     n.Available,
		topology:      topology,
		requests:      remainingDaemonResources,
		requirements:  scheduling.NewLabelRequirements(n.Node.Labels),
		hostPortUsage: n.HostPortUsage,
		volumeUsage:   n.VolumeUsage,
		volumeLimits:  n.VolumeLimits,
	}

	ephemeralTaints := []v1.Taint{
		{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoSchedule},
		{Key: v1.TaintNodeUnreachable, Effect: v1.TaintEffectNoSchedule},
	}
	// Only consider startup taints until the node is initialized. Without this, if the startup taint is generic and
	// re-appears on the node for a different reason (e.g. the node is cordoned) we will assume that pods can
	// schedule against the node in the future incorrectly.
	if n.Node.Labels[v1alpha5.LabelNodeInitialized] != "true" {
		ephemeralTaints = append(ephemeralTaints, startupTaints...)
	}

	// Filter out ignored taints
	node.taints = lo.Reject(n.Node.Spec.Taints, func(taint v1.Taint, _ int) bool {
		_, rejected := lo.Find(ephemeralTaints, func(t v1.Taint) bool {
			return t.Key == taint.Key && t.Value == taint.Value && t.Effect == taint.Effect
		})
		return rejected
	})

	// If the in-flight node doesn't have a hostname yet, we treat it's unique name as the hostname.  This allows toppology
	// with hostname keys to schedule correctly.
	hostname := n.Node.Labels[v1.LabelHostname]
	if hostname == "" {
		hostname = n.Node.Name
	}
	node.requirements.Add(scheduling.NewRequirement(v1.LabelHostname, v1.NodeSelectorOpIn, hostname))
	topology.Register(v1.LabelHostname, hostname)
	return node
}

func (n *ExistingMachine) Add(ctx context.Context, pod *v1.Pod) error {
	// Check Taints
	if err := scheduling.Taints(n.taints).Tolerates(pod); err != nil {
		return err
	}

	if err := n.hostPortUsage.Validate(pod); err != nil {
		return err
	}

	// determine the number of volumes that will be mounted if the pod schedules
	mountedVolumeCount, err := n.volumeUsage.Validate(ctx, pod)
	if err != nil {
		return err
	}
	if mountedVolumeCount.Exceeds(n.volumeLimits) {
		return fmt.Errorf("would exceed node volume limits")
	}

	// check resource requests first since that's a pretty likely reason the pod won't schedule on an in-flight
	// node, which at this point can't be increased in size
	requests := resources.Merge(n.requests, resources.RequestsForPods(pod))

	if !resources.Fits(requests, n.available) {
		return fmt.Errorf("exceeds node resources")
	}

	nodeRequirements := scheduling.NewRequirements(n.requirements.Values()...)
	podRequirements := scheduling.NewPodRequirements(pod)
	// Check Machine Affinity Requirements
	if err := nodeRequirements.Compatible(podRequirements); err != nil {
		return err
	}
	nodeRequirements.Add(podRequirements.Values()...)

	// Check Topology Requirements
	topologyRequirements, err := n.topology.AddRequirements(podRequirements, nodeRequirements, pod)
	if err != nil {
		return err
	}
	if err = nodeRequirements.Compatible(topologyRequirements); err != nil {
		return err
	}
	nodeRequirements.Add(topologyRequirements.Values()...)

	// Update node
	n.Pods = append(n.Pods, pod)
	n.requests = requests
	n.requirements = nodeRequirements
	n.topology.Record(pod, nodeRequirements)
	n.hostPortUsage.Add(ctx, pod)
	n.volumeUsage.Add(ctx, pod)
	return nil
}

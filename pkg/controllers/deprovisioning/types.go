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

package deprovisioning

import (
	"bytes"
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"knative.dev/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/cloudprovider"
	"github.com/aws/karpenter-core/pkg/controllers/provisioning/scheduling"
	"github.com/aws/karpenter-core/pkg/controllers/state"
)

type Deprovisioner interface {
	ShouldDeprovision(context.Context, *Candidate) bool
	ComputeCommand(context.Context, ...*Candidate) (Command, error)
	String() string
}

// Candidate is a state.Node that we are considering for deprovisioning along with extra information to be used in
// making that determination
type Candidate struct {
	*state.Node
	instanceType   *cloudprovider.InstanceType
	provisioner    *v1alpha5.Provisioner
	disruptionCost float64
	pods           []*v1.Pod
}

//nolint:gocyclo
func NewCandidateNode(ctx context.Context, kubeClient client.Client, clk clock.Clock, node *state.Node,
	provisionerMap map[string]*v1alpha5.Provisioner, provisionerToInstanceTypes map[string]map[string]*cloudprovider.InstanceType) *Candidate {

	// check whether the node has all the labels we need
	for _, label := range []string{
		v1alpha5.LabelCapacityType,
		v1.LabelTopologyZone,
		v1alpha5.ProvisionerNameLabelKey,
	} {
		if _, ok := node.Labels()[label]; !ok {
			return nil
		}
	}

	provisioner := provisionerMap[node.Labels()[v1alpha5.ProvisionerNameLabelKey]]
	instanceTypeMap := provisionerToInstanceTypes[node.Labels()[v1alpha5.ProvisionerNameLabelKey]]
	// skip any nodes where we can't determine the provisioner
	if provisioner == nil || instanceTypeMap == nil {
		return nil
	}

	instanceType := instanceTypeMap[node.Labels()[v1.LabelInstanceTypeStable]]
	// skip any nodes that we can't determine the instance of
	if instanceType == nil {
		return nil
	}

	// skip any nodes that are already marked for deletion and being handled
	if node.MarkedForDeletion() {
		return nil
	}
	// skip nodes that aren't initialized
	// This also means that the real Node doesn't exist for it
	if !node.Initialized() {
		return nil
	}
	// skip the node if it is nominated by a recent provisioning pass to be the target of a pending pod.
	if node.Nominated() {
		return nil
	}
	if node.Node == nil || node.Machine == nil {
		return nil
	}

	pods, err := node.Pods(ctx, kubeClient)
	if err != nil {
		logging.FromContext(ctx).Errorf("Determining node pods, %s", err)
		return nil
	}
	cn := &Candidate{
		Node:           node.DeepCopy(),
		instanceType:   instanceType,
		provisioner:    provisioner,
		pods:           pods,
		disruptionCost: disruptionCost(ctx, pods),
	}
	cn.disruptionCost *= lifetimeRemaining(cn, clk)
	return cn
}

// lifetimeRemaining calculates the fraction of node lifetime remaining in the range [0.0, 1.0].  If the TTLSecondsUntilExpired
// is non-zero, we use it to scale down the disruption costs of nodes that are going to expire.  Just after creation, the
// disruption cost is highest and it approaches zero as the node ages towards its expiration time.
func lifetimeRemaining(c *Candidate, clock clock.Clock) float64 {
	remaining := 1.0
	if c.provisioner.Spec.TTLSecondsUntilExpired != nil {
		ageInSeconds := clock.Since(c.Node.Node.CreationTimestamp.Time).Seconds()
		totalLifetimeSeconds := float64(*c.provisioner.Spec.TTLSecondsUntilExpired)
		lifetimeRemainingSeconds := totalLifetimeSeconds - ageInSeconds
		remaining = clamp(0.0, lifetimeRemainingSeconds/totalLifetimeSeconds, 1.0)
	}
	return remaining
}

type action byte

const (
	actionDelete action = iota
	actionReplace
	actionRetry
	actionDoNothing
)

func (a action) String() string {
	switch a {
	// Deprovisioning action with no replacement nodes
	case actionDelete:
		return "delete"
	// Deprovisioning action with replacement nodes
	case actionReplace:
		return "replace"
	// Deprovisioning failed for a retryable reason
	case actionRetry:
		return "retry"
	case actionDoNothing:
		return "do nothing"
	default:
		return fmt.Sprintf("unknown (%d)", a)
	}
}

type Command struct {
	candidatesToRemove  []*Candidate
	action              action
	replacementMachines []*scheduling.Machine
}

func (o Command) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s, terminating %d machines ", o.action, len(o.candidatesToRemove))
	for i, old := range o.candidatesToRemove {
		if i != 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprintf(&buf, "%s", old.Name())
		if instanceType, ok := old.Labels()[v1.LabelInstanceTypeStable]; ok {
			fmt.Fprintf(&buf, "/%s", instanceType)
		}
		if capacityType, ok := old.Labels()[v1alpha5.LabelCapacityType]; ok {
			fmt.Fprintf(&buf, "/%s", capacityType)
		}
	}
	if len(o.replacementMachines) == 0 {
		return buf.String()
	}
	odNodes := 0
	spotNodes := 0
	for _, machine := range o.replacementMachines {
		ct := machine.Requirements.Get(v1alpha5.LabelCapacityType)
		if ct.Has(v1alpha5.CapacityTypeOnDemand) {
			odNodes++
		}
		if ct.Has(v1alpha5.CapacityTypeSpot) {
			spotNodes++
		}
	}
	// Print list of instance types for the first replacementNode.
	if len(o.replacementMachines) > 1 {
		fmt.Fprintf(&buf, " and replacing with %d spot and %d on-demand nodes from types %s",
			spotNodes, odNodes,
			scheduling.InstanceTypeList(o.replacementMachines[0].InstanceTypeOptions))
		return buf.String()
	}
	ct := o.replacementMachines[0].Requirements.Get(v1alpha5.LabelCapacityType)
	nodeDesc := "node"
	if ct.Len() == 1 {
		nodeDesc = fmt.Sprintf("%s node", ct.Any())
	}
	fmt.Fprintf(&buf, " and replacing with %s from types %s",
		nodeDesc,
		scheduling.InstanceTypeList(o.replacementMachines[0].InstanceTypeOptions))
	return buf.String()
}

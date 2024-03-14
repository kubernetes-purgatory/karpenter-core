/*
Copyright The Kubernetes Authors.

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

package disruption

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/samber/lo"

	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/apis/v1beta1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/orchestration"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	operatorlogging "sigs.k8s.io/karpenter/pkg/operator/logging"
)

var errCandidateDeleting = fmt.Errorf("candidate is deleting")

//nolint:gocyclo
func SimulateScheduling(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner,
	candidates ...*Candidate,
) (pscheduling.Results, error) {
	candidateNames := sets.NewString(lo.Map(candidates, func(t *Candidate, i int) string { return t.Name() })...)
	nodes := cluster.Nodes()
	deletingNodes := nodes.Deleting()
	stateNodes := lo.Filter(nodes.Active(), func(n *state.StateNode, _ int) bool {
		return !candidateNames.Has(n.Name())
	})

	// We do one final check to ensure that the node that we are attempting to consolidate isn't
	// already handled for deletion by some other controller. This could happen if the node was markedForDeletion
	// between returning the candidates and getting the stateNodes above
	if _, ok := lo.Find(deletingNodes, func(n *state.StateNode) bool {
		return candidateNames.Has(n.Name())
	}); ok {
		return pscheduling.Results{}, errCandidateDeleting
	}

	// We get the pods that are on nodes that are deleting
	deletingNodePods, err := deletingNodes.ReschedulablePods(ctx, kubeClient)
	if err != nil {
		return pscheduling.Results{}, fmt.Errorf("failed to get pods from deleting nodes, %w", err)
	}
	// start by getting all pending pods
	pods, err := provisioner.GetPendingPods(ctx)
	if err != nil {
		return pscheduling.Results{}, fmt.Errorf("determining pending pods, %w", err)
	}
	for _, n := range candidates {
		pods = append(pods, n.reschedulablePods...)
	}
	pods = append(pods, deletingNodePods...)
	scheduler, err := provisioner.NewScheduler(log.IntoContext(ctx, operatorlogging.NopLogger), pods, stateNodes)
	if err != nil {
		return pscheduling.Results{}, fmt.Errorf("creating scheduler, %w", err)
	}

	deletingNodePodKeys := lo.SliceToMap(deletingNodePods, func(p *v1.Pod) (client.ObjectKey, interface{}) {
		return client.ObjectKeyFromObject(p), nil
	})

	results := scheduler.Solve(log.IntoContext(ctx, operatorlogging.NopLogger), pods).TruncateInstanceTypes(pscheduling.MaxInstanceTypes)
	for _, n := range results.ExistingNodes {
		// We consider existing nodes for scheduling. When these nodes are unmanaged, their taint logic should
		// tell us if we can schedule to them or not; however, if these nodes are managed, we will still schedule to them
		// even if they are still in the middle of their initialization loop. In the case of disruption, we don't want
		// to proceed disrupting if our scheduling decision relies on nodes that haven't entered a terminal state.
		if !n.Initialized() {
			for _, p := range n.Pods {
				// Only add a pod scheduling error if it isn't on an already deleting node.
				// If the pod is on a deleting node, we assume one of two things has already happened:
				// 1. The node was manually terminated, at which the provisioning controller has scheduled or is scheduling a node
				//    for the pod.
				// 2. The node was chosen for a previous disruption command, we assume that the uninitialized node will come up
				//    for this command, and we assume it will be successful. If it is not successful, the node will become
				//    not terminating, and we will no longer need to consider these pods.
				if _, ok := deletingNodePodKeys[client.ObjectKeyFromObject(p)]; !ok {
					results.PodErrors[p] = NewUninitializedNodeError(n)
				}
			}
		}
	}
	return results, nil
}

// UninitializedNodeError tracks a special pod error for disruption where pods schedule to a node
// that hasn't been initialized yet, meaning that we can't be confident to make a disruption decision based off of it
type UninitializedNodeError struct {
	*pscheduling.ExistingNode
}

func NewUninitializedNodeError(node *pscheduling.ExistingNode) *UninitializedNodeError {
	return &UninitializedNodeError{ExistingNode: node}
}

func (u *UninitializedNodeError) Error() string {
	var info []string
	if u.NodeClaim != nil {
		info = append(info, fmt.Sprintf("nodeclaim/%s", u.NodeClaim.Name))
	}
	if u.Node != nil {
		info = append(info, fmt.Sprintf("node/%s", u.Node.Name))
	}
	return fmt.Sprintf("would schedule against uninitialized %s", strings.Join(info, ", "))
}

// instanceTypesAreSubset returns true if the lhs slice of instance types are a subset of the rhs.
func instanceTypesAreSubset(lhs []*cloudprovider.InstanceType, rhs []*cloudprovider.InstanceType) bool {
	rhsNames := sets.NewString(lo.Map(rhs, func(t *cloudprovider.InstanceType, i int) string { return t.Name })...)
	lhsNames := sets.NewString(lo.Map(lhs, func(t *cloudprovider.InstanceType, i int) string { return t.Name })...)
	return len(rhsNames.Intersection(lhsNames)) == len(lhsNames)
}

// GetCandidates returns nodes that appear to be currently deprovisionable based off of their nodePool
func GetCandidates(ctx context.Context, cluster *state.Cluster, kubeClient client.Client, recorder events.Recorder, clk clock.Clock,
	cloudProvider cloudprovider.CloudProvider, shouldDeprovision CandidateFilter, queue *orchestration.Queue,
) ([]*Candidate, error) {
	nodePoolMap, nodePoolToInstanceTypesMap, err := BuildNodePoolMap(ctx, kubeClient, cloudProvider)
	if err != nil {
		return nil, err
	}
	pdbs, err := pdb.NewLimits(ctx, clk, kubeClient)
	if err != nil {
		return nil, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
	}
	candidates := lo.FilterMap(cluster.Nodes(), func(n *state.StateNode, _ int) (*Candidate, bool) {
		cn, e := NewCandidate(ctx, kubeClient, recorder, clk, n, pdbs, nodePoolMap, nodePoolToInstanceTypesMap, queue)
		return cn, e == nil
	})
	// Filter only the valid candidates that we should disrupt
	return lo.Filter(candidates, func(c *Candidate, _ int) bool { return shouldDeprovision(ctx, c) }), nil
}

// BuildNodePoolMap builds a provName -> nodePool map and a provName -> instanceName -> instance type map
func BuildNodePoolMap(ctx context.Context, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) (map[string]*v1beta1.NodePool, map[string]map[string]*cloudprovider.InstanceType, error) {
	nodePoolMap := map[string]*v1beta1.NodePool{}
	nodePoolList := &v1beta1.NodePoolList{}
	if err := kubeClient.List(ctx, nodePoolList); err != nil {
		return nil, nil, fmt.Errorf("listing node pools, %w", err)
	}
	nodePoolToInstanceTypesMap := map[string]map[string]*cloudprovider.InstanceType{}
	for i := range nodePoolList.Items {
		np := &nodePoolList.Items[i]
		nodePoolMap[np.Name] = np

		nodePoolInstanceTypes, err := cloudProvider.GetInstanceTypes(ctx, np)
		if err != nil {
			// don't error out on building the node pool, we just won't be able to handle any nodes that
			// were created by it
			log.FromContext(ctx).Error(err, fmt.Sprintf("failed listing instance types for %s", np.Name))
			continue
		}
		if len(nodePoolInstanceTypes) == 0 {
			continue
		}
		nodePoolToInstanceTypesMap[np.Name] = map[string]*cloudprovider.InstanceType{}
		for _, it := range nodePoolInstanceTypes {
			nodePoolToInstanceTypesMap[np.Name][it.Name] = it
		}
	}
	return nodePoolMap, nodePoolToInstanceTypesMap, nil
}

// BuildDisruptionBudgets prepares our disruption budget mapping. The disruption budget maps for each disruption reason the number of allowed disruptions for each node pool.
// We calculate allowed disruptions by taking the max disruptions allowed by disruption reason and subtracting the number of nodes that are NotReady and already being deleted by that disruption reason.
//
//nolint:gocyclo
func BuildDisruptionBudgets(ctx context.Context, cluster *state.Cluster, clk clock.Clock, kubeClient client.Client, recorder events.Recorder) (map[string]map[v1beta1.DisruptionReason]int, error) {
	disruptionBudgetMapping := map[string]map[v1beta1.DisruptionReason]int{}
	numNodes := map[string]int{}   // map[nodepool] -> node count in nodepool
	disrupting := map[string]int{} // map[nodepool] -> nodes undergoing disruption
	for _, node := range cluster.Nodes() {
		// We only consider nodes that we own and are initialized towards the total.
		// If a node is launched/registered, but not initialized, pods aren't scheduled
		// to the node, and these are treated as unhealthy until they're cleaned up.
		// This prevents odd roundup cases with percentages where replacement nodes that
		// aren't initialized could be counted towards the total, resulting in more disruptions
		// to active nodes than desired, where Karpenter should wait for these nodes to be
		// healthy before continuing.
		if !node.Managed() || !node.Initialized() {
			continue
		}

		nodePool := node.Labels()[v1beta1.NodePoolLabelKey]
		numNodes[nodePool]++

		// If the node satisfies one of the following, we subtract it from the allowed disruptions.
		// 1. Has a NotReady conditiion
		// 2. Is marked as disrupting
		if cond := nodeutils.GetCondition(node.Node, v1.NodeReady); cond.Status != v1.ConditionTrue || node.MarkedForDeletion() {
			disrupting[nodePool]++
		}
	}
	nodePoolList := &v1beta1.NodePoolList{}
	if err := kubeClient.List(ctx, nodePoolList); err != nil {
		return nil, fmt.Errorf("listing node pools, %w", err)
	}
	for _, nodePool := range nodePoolList.Items {
		minDisruptionsByReason := nodePool.MustGetAllowedDisruptions(ctx, clk, numNodes[nodePool.Name])
		allowedDisruptionsTotal := 0

		disruptionBudgetMapping[nodePool.Name] = map[v1beta1.DisruptionReason]int{}
		for reason, minDisruptions := range minDisruptionsByReason {
			// Subtract the allowed number of disruptions from the number of already disrupting nodes.
			// Floor the value since the number of disrupting nodes can exceed the number of allowed disruptions.
			// Allowing this value to be negative breaks assumptions in the code used to calculate how many nodes can be disrupted.
			allowedDisruptions := lo.Clamp(minDisruptions-disrupting[nodePool.Name], 0, math.MaxInt32)
			disruptionBudgetMapping[nodePool.Name][reason] = allowedDisruptions

			allowedDisruptionsTotal += allowedDisruptions
			BudgetsAllowedDisruptionsGauge.With(map[string]string{
				metrics.NodePoolLabel: nodePool.Name, metrics.ReasonLabel: string(reason),
			}).Set(float64(allowedDisruptions))
		}
		if allowedDisruptionsTotal == 0 {
			recorder.Publish(disruptionevents.NodePoolBlocked(lo.ToPtr(nodePool)))
		}
	}
	return disruptionBudgetMapping, nil
}

// mapCandidates maps the list of proposed candidates with the current state
func mapCandidates(proposed, current []*Candidate) []*Candidate {
	proposedNames := sets.NewString(lo.Map(proposed, func(c *Candidate, i int) string { return c.Name() })...)
	return lo.Filter(current, func(c *Candidate, _ int) bool {
		return proposedNames.Has(c.Name())
	})
}

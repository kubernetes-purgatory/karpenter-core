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
	"errors"
	"sort"

	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

// Drift is a subreconciler that deletes drifted candidates.
type EventualDisruption struct {
	kubeClient  client.Client
	cluster     *state.Cluster
	provisioner *provisioning.Provisioner
	recorder    events.Recorder
	reason      v1.DisruptionReason
}

func NewEventualDisruption(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, recorder events.Recorder, reason v1.DisruptionReason) *EventualDisruption {
	return &EventualDisruption{
		kubeClient:  kubeClient,
		cluster:     cluster,
		provisioner: provisioner,
		recorder:    recorder,
		reason:      reason,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *EventualDisruption) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	return c.NodeClaim.StatusConditions().Get(string(d.Reason())).IsTrue()
}

// ComputeCommand generates a disruption command given candidates
func (d *EventualDisruption) ComputeCommand(ctx context.Context, disruptionBudgetMapping map[string]map[v1.DisruptionReason]int, candidates ...*Candidate) (Command, scheduling.Results, error) {
	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time.Before(
			candidates[j].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time)
	})

	// Do a quick check through the candidates to see if they're empty.
	// For each candidate that is empty with a nodePool allowing its disruption
	// add it to the existing command.
	empty := make([]*Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		_, found := disruptionBudgetMapping[candidate.nodePool.Name][d.Reason()]
		reason := lo.Ternary(found, d.Reason(), v1.DisruptionReasonAll)
		if len(candidate.reschedulablePods) > 0 {
			continue
		}
		// If there's disruptions allowed for the candidate's nodepool,
		// add it to the list of candidates, and decrement the budget.
		if disruptionBudgetMapping[candidate.nodePool.Name][reason] > 0 {
			empty = append(empty, candidate)
			disruptionBudgetMapping[candidate.nodePool.Name][reason]--
		}
	}
	// Disrupt all empty drifted candidates, as they require no scheduling simulations.
	if len(empty) > 0 {
		return Command{
			candidates: empty,
		}, scheduling.Results{}, nil
	}

	for _, candidate := range candidates {
		_, found := disruptionBudgetMapping[candidate.nodePool.Name][d.Reason()]
		reason := lo.Ternary(found, d.Reason(), v1.DisruptionReasonAll)
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since drift commands can only have one candidate.
		if disruptionBudgetMapping[candidate.nodePool.Name][reason] == 0 {
			continue
		}
		// Check if we need to create any NodeClaims.
		results, err := SimulateScheduling(ctx, d.kubeClient, d.cluster, d.provisioner, candidate)
		if err != nil {
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return Command{}, scheduling.Results{}, err
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, results.NonPendingPodSchedulingErrors())...)
			continue
		}

		return Command{
			candidates:   []*Candidate{candidate},
			replacements: results.NewNodeClaims,
		}, results, nil
	}
	return Command{}, scheduling.Results{}, nil
}

func (d *EventualDisruption) Reason() v1.DisruptionReason {
	return d.reason
}

func (d *EventualDisruption) Class() string {
	return EventualDisruptionClass
}

func (d *EventualDisruption) ConsolidationType() string {
	return ""
}

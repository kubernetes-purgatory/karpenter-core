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

package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/multierr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	"knative.dev/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aws/karpenter-core/pkg/controllers/provisioning"
	"github.com/aws/karpenter-core/pkg/operator/controller"

	"github.com/aws/karpenter-core/pkg/apis/v1beta1"
	disruptionevents "github.com/aws/karpenter-core/pkg/controllers/disruption/events"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/events"
	"github.com/aws/karpenter-core/pkg/utils/nodeclaim"
)

const (
	queueBaseDelay   = 1 * time.Second
	queueMaxDelay    = 10 * time.Second
	maxRetryDuration = 10 * time.Minute
)

type Command struct {
	ReplacementKeys   []*NodeClaimKey
	Candidates        []*state.StateNode
	Method            string    // used for metrics
	ConsolidationType string    // used for metrics
	TimeAdded         time.Time // timeAdded is used to track timeouts
	LastError         error
}

// NodeClaimKey wraps a nodeclaim.Key with an initialized field to save on readiness checks and identify
// when a nodeclaim is first initialized for metrics and events.
type NodeClaimKey struct {
	nodeclaim.Key
	// Use a bool track if a node has already been initialized so we can fire metrics for intialization once.
	// This intentionally does not capture nodes that go initialized then go NotReady after as other pods can
	// schedule to this node as well.
	Initialized bool
}

type UnrecoverableError struct {
	error
}

func NewUnrecoverableError(err error) *UnrecoverableError {
	return &UnrecoverableError{error: err}
}

func IsUnrecoverableError(err error) bool {
	if err == nil {
		return false
	}
	var unrecoverableError *UnrecoverableError
	return errors.As(err, &unrecoverableError)
}

type Queue struct {
	workqueue.RateLimitingInterface
	// providerID -> command, maps a candidate to its command. Each command has a list of candidates that can be used
	// to map to itself.
	ProviderIDToCommand map[string]*Command

	kubeClient  client.Client
	recorder    events.Recorder
	cluster     *state.Cluster
	clock       clock.Clock
	provisioner *provisioning.Provisioner
}

// NewQueue creates a queue that will asynchronously orchestrate disruption commands
func NewQueue(kubeClient client.Client, recorder events.Recorder, cluster *state.Cluster, clock clock.Clock,
	provisioner *provisioning.Provisioner) *Queue {
	queue := &Queue{
		RateLimitingInterface: workqueue.NewRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(queueBaseDelay, queueMaxDelay)),
		ProviderIDToCommand:   map[string]*Command{},
		kubeClient:            kubeClient,
		recorder:              recorder,
		cluster:               cluster,
		clock:                 clock,
		provisioner:           provisioner,
	}
	return queue
}

// NewCommand creates a command key and adds in initial data for the orchestration queue.
func NewCommand(replacements []nodeclaim.Key, candidates []*state.StateNode, timeAdded time.Time, reason string, consolidationType string) *Command {
	return &Command{
		ReplacementKeys: lo.Map(replacements, func(key nodeclaim.Key, _ int) *NodeClaimKey {
			return &NodeClaimKey{Key: key}
		}),
		Candidates:        candidates,
		Method:            reason,
		ConsolidationType: consolidationType,
		TimeAdded:         timeAdded,
	}
}

func (q *Queue) Name() string {
	return "disruption.queue"
}

func (q *Queue) Builder(_ context.Context, m manager.Manager) controller.Builder {
	return controller.NewSingletonManagedBy(m)
}

func (q *Queue) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	// Check if the queue is empty. client-go recommends not using this function to gate the subsequent
	// get call, but since we're popping items off the queue synchronously, there should be no synchonization
	// issues.
	for q.Len() == 0 {
		return reconcile.Result{RequeueAfter: 1 * time.Second}, nil
	}
	// Get command from queue. This waits until queue is non-empty.
	item, shutdown := q.RateLimitingInterface.Get()
	if shutdown {
		return reconcile.Result{}, fmt.Errorf("disruption queue has shut down")
	}
	cmd := item.(*Command)
	defer q.RateLimitingInterface.Done(cmd)

	if err := q.Process(ctx, cmd); err != nil {
		// If recoverable, re-queue and try again.
		if !IsUnrecoverableError(err) {
			// log the error that is causing us to fail so we can bubble it up later if this times out.
			cmd.LastError = err
			q.RateLimitingInterface.AddRateLimited(cmd)
			return reconcile.Result{RequeueAfter: controller.Immediately}, nil
		}
		// If the command failed, bail on the action.
		// 1. Emit metrics for launch failures
		// 2. Ensure cluster state no longer thinks these nodes are deleting
		// 3. Remove it from the Queue's internal data structure
		failedLaunches := lo.Filter(cmd.ReplacementKeys, func(key *NodeClaimKey, _ int) bool {
			return !key.Initialized
		})
		disruptionReplacementNodeClaimFailedCounter.With(map[string]string{
			methodLabel:            cmd.Method,
			consolidationTypeLabel: cmd.ConsolidationType,
		}).Add(float64(len(failedLaunches)))
		q.cluster.UnmarkForDeletion(lo.Map(cmd.Candidates, func(s *state.StateNode, _ int) string { return s.ProviderID() })...)
		multiErr := multierr.Combine(err, state.RequireNoScheduleTaint(ctx, q.kubeClient, false, cmd.Candidates...), cmd.LastError)

		nodeNames := strings.Join(lo.Map(cmd.Candidates, func(s *state.StateNode, _ int) string {
			return s.Name()
		}), ",")
		q.Remove(cmd)
		logging.FromContext(ctx).With("nodeNames", nodeNames).Errorf("failed to disrupt nodes, %w", multiErr)
		return reconcile.Result{RequeueAfter: controller.Immediately}, nil
	}
	// If command is complete, remove command from queue.
	q.Remove(cmd)
	return reconcile.Result{RequeueAfter: controller.Immediately}, nil
}

func (q *Queue) Process(ctx context.Context, cmd *Command) error {
	if q.clock.Since(cmd.TimeAdded) > maxRetryDuration {
		return NewUnrecoverableError(fmt.Errorf("command at %s reached timeout after %s", cmd.TimeAdded, q.clock.Since(cmd.TimeAdded)))
	}
	// If the time hasn't expired, either wait or terminate.
	if err := q.WaitOrTerminate(ctx, cmd); err != nil {
		// If there was an error, set this as the command's last error so that we can propagate it.
		if IsUnrecoverableError(err) {
			return fmt.Errorf("got unrecoverable error, %w", err)
		}
		return err
	}
	return nil
}

// WaitOrTerminate will wait until launched nodeclaims are ready.
// Once the replacements are ready, it will terminate the candidates.
// Will return true if the item in the queue should be re-queued. If a command has
// timed out, this will return false.
// nolint:gocyclo
func (q *Queue) WaitOrTerminate(ctx context.Context, cmd *Command) error {
	waitErrs := make([]error, len(cmd.ReplacementKeys))
	for i := range cmd.ReplacementKeys {
		key := cmd.ReplacementKeys[i]
		// If we know the node claim is initialized, no need to check again.
		if key.Initialized {
			continue
		}
		// Get the nodeclaim
		nodeClaim, err := nodeclaim.Get(ctx, q.kubeClient, key.Key)
		if err != nil {
			// The NodeClaim got deleted after an initial eventual consistency delay
			// This means that there was an ICE error or the Node initializationTTL expired
			// In this case, the error is unrecoverable, so don't requeue.
			if apierrors.IsNotFound(err) && q.clock.Since(cmd.TimeAdded) > time.Second*5 {
				return NewUnrecoverableError(err)
			}
			waitErrs[i] = fmt.Errorf("getting node claim, %w", err)
			continue
		}
		// We emitted this event when disruption was blocked on launching/termination.
		// This does not block other forms of deprovisioning, but we should still emit this.
		q.recorder.Publish(disruptionevents.Launching(nodeClaim, fmt.Sprintf("%s/%s", cmd.Method,
			lo.Ternary(len(cmd.ReplacementKeys) > 0, "replace", "delete"))))
		initializedStatus := nodeClaim.StatusConditions().GetCondition(v1beta1.Initialized)
		if !initializedStatus.IsTrue() {
			q.recorder.Publish(disruptionevents.WaitingOnReadiness(nodeClaim))
			waitErrs[i] = fmt.Errorf("node claim not initialized")
			continue
		}
		cmd.ReplacementKeys[i].Initialized = true
		// Subtract the last initialization time from the time the command was added to get initialization duration.
		initLength := initializedStatus.LastTransitionTime.Inner.Time.Sub(nodeClaim.CreationTimestamp.Time).Seconds()
		disruptionReplacementNodeClaimInitializedHistogram.Observe(initLength)
	}
	// If we have any errors, don't continue
	if err := multierr.Combine(waitErrs...); err != nil {
		return fmt.Errorf("waiting for replacement initialization, %w", err)
	}

	// All replacements have been provisioned.
	// All we need to do now is get a successful delete call for each node claim,
	// then the termination controller will handle the eventual deletion of the nodes.
	var multiErr error
	for i := range cmd.Candidates {
		candidate := cmd.Candidates[i]
		q.recorder.Publish(disruptionevents.Terminating(candidate.Node, candidate.NodeClaim, fmt.Sprintf("%s/%s", cmd.Method,
			lo.Ternary(len(cmd.ReplacementKeys) > 0, "replace", "delete")))...)
		if err := nodeclaim.Delete(ctx, q.kubeClient, candidate.NodeClaim); err != nil {
			multiErr = multierr.Append(multiErr, client.IgnoreNotFound(err))
		} else {
			nodeclaim.TerminatedCounter(cmd.Candidates[i].NodeClaim, cmd.Method).Inc()
		}
	}
	// If there were any deletion failures, we should requeue.
	// In the case where we requeue, but the timeout for the command is reached, we'll mark this as a failure.
	if multiErr != nil {
		return fmt.Errorf("terminating nodeclaims, %w", multiErr)
	}
	return nil
}

// Add adds commands to the Queue
// Each command added to the queue should already be validated and ready for execution.
func (q *Queue) Add(cmd *Command) error {
	providerIDs := lo.Map(cmd.Candidates, func(s *state.StateNode, _ int) string {
		return s.ProviderID()
	})
	// First check if we can add the command.
	if err := q.CanAdd(providerIDs...); err != nil {
		return fmt.Errorf("adding command, %w", err)
	}
	for _, candidate := range cmd.Candidates {
		q.ProviderIDToCommand[candidate.ProviderID()] = cmd
	}
	// Idempotently mark for deletion
	q.cluster.MarkForDeletion(providerIDs...)
	q.RateLimitingInterface.Add(cmd)
	return nil
}

// CanAdd is a quick check to see if the candidate is already part of a disruption action
func (q *Queue) CanAdd(ids ...string) error {
	var err error
	for _, id := range ids {
		if _, ok := q.ProviderIDToCommand[id]; ok {
			err = multierr.Append(err, fmt.Errorf("candidate is being disrupted"))
		}
	}
	return err
}

// Remove fully clears the queue of all references of a hash/command
func (q *Queue) Remove(cmd *Command) {
	// Remove all candidates linked to the command
	for _, candidate := range cmd.Candidates {
		delete(q.ProviderIDToCommand, candidate.ProviderID())
	}
	q.RateLimitingInterface.Forget(cmd)
	q.RateLimitingInterface.Done(cmd)
}

// Reset is used for testing and clears all internal data structures
func (q *Queue) Reset() {
	q.RateLimitingInterface = workqueue.NewRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(queueBaseDelay, queueMaxDelay))
	q.ProviderIDToCommand = map[string]*Command{}
}

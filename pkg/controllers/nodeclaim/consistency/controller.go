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

package consistency

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	nodeclaimutil "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

type Controller struct {
	clock       clock.Clock
	kubeClient  client.Client
	checks      []Check
	recorder    events.Recorder
	lastScanned *cache.Cache
}

type Issue string

type Check interface {
	// Check performs the consistency check, this should return a list of slice discovered, or an empty
	// slice if no issues were found
	Check(context.Context, *corev1.Node, *v1.NodeClaim) ([]Issue, error)
}

// scanPeriod is how often we inspect and report issues that are found.
const scanPeriod = 10 * time.Minute

func NewController(clk clock.Clock, kubeClient client.Client, recorder events.Recorder) *Controller {
	return &Controller{
		clock:       clk,
		kubeClient:  kubeClient,
		recorder:    recorder,
		lastScanned: cache.New(scanPeriod, 1*time.Minute),
		checks: []Check{
			NewTermination(clk, kubeClient),
			NewNodeShape(),
		},
	}
}

func (c *Controller) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, "nodeclaim.consistency")

	if nodeClaim.Status.ProviderID == "" {
		return reconcile.Result{}, nil
	}
	stored := nodeClaim.DeepCopy()
	// If we get an event before we should check for consistency checks, we ignore and wait
	if lastTime, ok := c.lastScanned.Get(string(nodeClaim.UID)); ok {
		if lastTime, ok := lastTime.(time.Time); ok {
			remaining := scanPeriod - c.clock.Since(lastTime)
			return reconcile.Result{RequeueAfter: remaining}, nil
		}
		// the above should always succeed
		return reconcile.Result{RequeueAfter: scanPeriod}, nil
	}
	c.lastScanned.SetDefault(string(nodeClaim.UID), c.clock.Now())

	// We assume the invariant that there is a single node for a single nodeClaim. If this invariant is violated,
	// then we assume this is bubbled up through the nodeClaim lifecycle controller and don't perform consistency checks
	node, err := nodeclaimutil.NodeForNodeClaim(ctx, c.kubeClient, nodeClaim)
	if err != nil {
		return reconcile.Result{}, nodeclaimutil.IgnoreDuplicateNodeError(nodeclaimutil.IgnoreNodeNotFoundError(err))
	}
	if err = c.checkConsistency(ctx, nodeClaim, node); err != nil {
		return reconcile.Result{}, err
	}
	if !equality.Semantic.DeepEqual(stored, nodeClaim) {
		// We call Update() here rather than Patch() because patching a list with a JSON merge patch
		// can cause races due to the fact that it fully replaces the list on a change
		// Here, we are updating the status condition list
		if err = c.kubeClient.Status().Update(ctx, nodeClaim); client.IgnoreNotFound(err) != nil {
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{RequeueAfter: scanPeriod}, nil
}

func (c *Controller) checkConsistency(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) error {
	hasIssues := false
	for _, check := range c.checks {
		issues, err := check.Check(ctx, node, nodeClaim)
		if err != nil {
			return fmt.Errorf("checking node with %T, %w", check, err)
		}
		for _, issue := range issues {
			log.FromContext(ctx).Error(stderrors.New(string(issue)), "consistency error")
			c.recorder.Publish(FailedConsistencyCheckEvent(nodeClaim, string(issue)))
		}
		hasIssues = hasIssues || (len(issues) > 0)
	}
	// If status condition for consistent state is not true and no issues are found, set the status condition to true
	if !nodeClaim.StatusConditions().IsTrue(v1.ConditionTypeConsistentStateFound) && !hasIssues {
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsistentStateFound)
	}
	// If there are issues then set the status condition for consistent state as false
	if hasIssues {
		nodeClaim.StatusConditions().SetFalse(v1.ConditionTypeConsistentStateFound, "ConsistencyCheckFailed", "Consistency Check Failed")
	}
	return nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("nodeclaim.consistency").
		For(&v1.NodeClaim{}).
		Watches(
			&corev1.Node{},
			nodeclaimutil.NodeEventHandler(c.kubeClient),
		).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

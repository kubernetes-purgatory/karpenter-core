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

package instancetermination

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	terminationreconcile "sigs.k8s.io/karpenter/pkg/controllers/node/termination/reconcile"
	"sigs.k8s.io/karpenter/pkg/metrics"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	terminationutils "sigs.k8s.io/karpenter/pkg/utils/termination"
)

type Controller struct {
	clk           clock.Clock
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
}

func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) *Controller {
	return &Controller{
		clk:           clk,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
	}
}

func (*Controller) Name() string {
	return "node.instancetermination"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.Node{}, builder.WithPredicates(nodeutils.IsManagedPredicateFuncs(c.cloudProvider))).
		Watches(&v1.NodeClaim{}, nodeutils.NodeClaimEventHandler(c.kubeClient, c.cloudProvider)).
		WithOptions(
			controller.Options{
				RateLimiter: workqueue.NewTypedMaxOfRateLimiter[reconcile.Request](
					workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](100*time.Millisecond, 10*time.Second),
					// 10 qps, 100 bucket size
					&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
				),
				MaxConcurrentReconciles: 100,
			},
		).
		Complete(terminationreconcile.AsReconciler(m.GetClient(), c.cloudProvider, c.clk, c))
}

func (*Controller) AwaitFinalizers() []string {
	return []string{v1.DrainFinalizer, v1.VolumeFinalizer}
}

func (*Controller) Finalizer() string {
	return v1.TerminationFinalizer
}

func (*Controller) NodeClaimNotFoundPolicy() terminationreconcile.Policy {
	return terminationreconcile.PolicyContinue
}

func (*Controller) TerminationGracePeriodPolicy() terminationreconcile.Policy {
	return terminationreconcile.PolicyContinue
}

func (c *Controller) Reconcile(ctx context.Context, n *corev1.Node, nc *v1.NodeClaim) (reconcile.Result, error) {
	isInstanceTerminated, err := terminationutils.EnsureTerminated(ctx, c.kubeClient, nc, c.cloudProvider)
	if err != nil {
		// 404 = the nodeClaim no longer exists
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		// 409 - The nodeClaim exists, but its status has already been modified
		if errors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, fmt.Errorf("ensuring instance termination, %w", err)
	}
	if !isInstanceTerminated {
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}

	stored := n.DeepCopy()
	controllerutil.RemoveFinalizer(n, c.Finalizer())
	// We use client.StrategicMergeFrom here since the node object supports it and
	// a strategic merge patch represents the finalizer list as a keyed "set" so removing
	// an item from the list doesn't replace the full list
	// https://github.com/kubernetes/kubernetes/issues/111643#issuecomment-2016489732
	if err := c.kubeClient.Patch(ctx, n, client.StrategicMergeFrom(stored)); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("removing %q finalizer, %w", c.Finalizer(), err)
	}
	metrics.NodesTerminatedTotal.Inc(map[string]string{
		metrics.NodePoolLabel: n.Labels[v1.NodePoolLabelKey],
	})
	// We use stored.DeletionTimestamp since the api-server may give back a node after the patch without a deletionTimestamp
	DurationSeconds.Observe(time.Since(stored.DeletionTimestamp.Time).Seconds(), map[string]string{
		metrics.NodePoolLabel: n.Labels[v1.NodePoolLabelKey],
	})
	NodeLifetimeDurationSeconds.Observe(time.Since(n.CreationTimestamp.Time).Seconds(), map[string]string{
		metrics.NodePoolLabel: n.Labels[v1.NodePoolLabelKey],
	})
	log.FromContext(ctx).Info("deleted node")
	return reconcile.Result{}, nil
}

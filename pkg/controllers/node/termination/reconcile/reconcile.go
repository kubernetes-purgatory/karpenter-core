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

package reconcile

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

func AsReconciler(c client.Client, cp cloudprovider.CloudProvider, clk clock.Clock, r Reconciler) reconcile.Reconciler {
	return reconcile.AsReconciler(c, &reconcilerAdapter{
		Reconciler:    r,
		kubeClient:    c,
		cloudProvider: cp,
		clk:           clk,
	})
}

type Reconciler interface {
	AwaitFinalizers() []string
	Finalizer() string

	Name() string
	Reconcile(context.Context, *corev1.Node, *v1.NodeClaim) (reconcile.Result, error)

	NodeClaimNotFoundPolicy() Policy
	TerminationGracePeriodPolicy() Policy
}

type Policy int

const (
	// If the condition associated with this policy is true, the reconcilers's associated finalizer will be removed and
	// reconciler will not be reconciled.
	PolicyFinalize Policy = iota
	// If the condition associated with this policy is true, the reconciler will be reconciled normally.
	PolicyContinue
)

type reconcilerAdapter struct {
	Reconciler

	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	clk           clock.Clock
}

//nolint:gocyclo
func (r *reconcilerAdapter) Reconcile(ctx context.Context, n *corev1.Node) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, r.Name())
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("Node", klog.KRef(n.Namespace, n.Name)))

	if n.GetDeletionTimestamp().IsZero() {
		return reconcile.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(n, r.Finalizer()) {
		return reconcile.Result{}, nil
	}
	if !nodeutils.IsManaged(n, r.cloudProvider) {
		return reconcile.Result{}, nil
	}

	for _, finalizer := range r.AwaitFinalizers() {
		if ok, err := nodeutils.ContainsFinalizer(n, finalizer); err != nil {
			// If any of the awaited finalizers require hydration, and the resources haven't been hydrated since the
			// last Karpenter update, we should short-circuit. The resource should be re-reconciled once hydrated.
			if nodeutils.IsHydrationError(err) {
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, err
		} else if ok {
			return reconcile.Result{}, nil
		}
	}

	nc, err := nodeutils.NodeClaimForNode(ctx, r.kubeClient, n)
	if err != nil {
		if nodeutils.IsDuplicateNodeClaimError(err) || nodeutils.IsNodeClaimNotFoundError(err) {
			log.FromContext(ctx).Error(err, "failed to terminate node")
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("NodeClaim", klog.KRef(nc.Namespace, nc.Name)))
	if nc.DeletionTimestamp.IsZero() {
		if err := r.kubeClient.Delete(ctx, nc); err != nil {
			if errors.IsNotFound(err) {
				log.FromContext(ctx).Error(err, "failed to terminate node")
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, err
		}
	}

	// If the underlying NodeClaim no longer exists, we want to delete to avoid trying to gracefully draining
	// on nodes that are no longer alive. We do a check on the Ready condition of the node since, even
	// though the CloudProvider says the instance is not around, we know that the kubelet process is still running
	// if the Node Ready condition is true
	// Similar logic to: https://github.com/kubernetes/kubernetes/blob/3a75a8c8d9e6a1ebd98d8572132e675d4980f184/staging/src/k8s.io/cloud-provider/controllers/nodelifecycle/node_lifecycle_controller.go#L144
	if r.NodeClaimNotFoundPolicy() == PolicyFinalize {
		if nodeutils.GetCondition(n, corev1.NodeReady).Status != corev1.ConditionTrue {
			if _, err := r.cloudProvider.Get(ctx, n.Spec.ProviderID); err != nil {
				if cloudprovider.IsNodeClaimNotFoundError(err) {
					return reconcile.Result{}, client.IgnoreNotFound(RemoveFinalizer(ctx, r.kubeClient, n, r.Finalizer()))
				}
				return reconcile.Result{}, fmt.Errorf("getting nodeclaim, %w", err)
			}
		}
	}

	if r.TerminationGracePeriodPolicy() == PolicyFinalize {
		if elapsed, err := nodeclaimutils.HasTerminationGracePeriodElapsed(r.clk, nc); err != nil {
			log.FromContext(ctx).Error(err, "failed to terminate node")
			return reconcile.Result{}, nil
		} else if elapsed {
			return reconcile.Result{}, client.IgnoreNotFound(RemoveFinalizer(ctx, r.kubeClient, n, r.Finalizer()))
		}
	}

	return r.Reconciler.Reconcile(ctx, n, nc)
}

func RemoveFinalizer(ctx context.Context, c client.Client, n *corev1.Node, finalizer string) error {
	stored := n.DeepCopy()
	if !controllerutil.RemoveFinalizer(n, finalizer) {
		return nil
	}
	if err := c.Patch(ctx, n, client.StrategicMergeFrom(stored)); err != nil {
		return err
	}
	return nil
}

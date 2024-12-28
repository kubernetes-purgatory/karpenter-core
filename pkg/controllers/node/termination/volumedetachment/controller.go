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

package volumedetachment

import (
	"context"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	terminationreconcile "sigs.k8s.io/karpenter/pkg/controllers/node/termination/reconcile"
	"sigs.k8s.io/karpenter/pkg/events"

	storagev1 "k8s.io/api/storage/v1"

	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	volumeutil "sigs.k8s.io/karpenter/pkg/utils/volume"
)

type Controller struct {
	clk           clock.Clock
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	recorder      events.Recorder
}

func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, recorder events.Recorder) *Controller {
	return &Controller{
		clk:           clk,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		recorder:      recorder,
	}
}

func (*Controller) Name() string {
	return "node.volumedetachment"
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
	return nil
}

func (*Controller) Finalizer() string {
	return v1.VolumeFinalizer
}

func (*Controller) TerminationGracePeriodPolicy() terminationreconcile.Policy {
	return terminationreconcile.PolicyFinalize
}

func (*Controller) NodeClaimNotFoundPolicy() terminationreconcile.Policy {
	return terminationreconcile.PolicyFinalize
}

// nolint:gocyclo
func (c *Controller) Reconcile(ctx context.Context, n *corev1.Node, nc *v1.NodeClaim) (reconcile.Result, error) {
	blockingVAs, err := c.blockingVolumeAttachments(ctx, n)
	if err != nil {
		return reconcile.Result{}, err
	}
	if len(blockingVAs) != 0 {
		stored := nc.DeepCopy()
		_ = nc.StatusConditions().SetFalse(v1.ConditionTypeVolumesDetached, "AwaitingVolumeDetachment", "AwaitingVolumeDetachment")
		if err := c.kubeClient.Status().Patch(ctx, nc, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			if errors.IsNotFound(err) {
				return reconcile.Result{}, nil
			}
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, err
		}
		c.recorder.Publish(NodeAwaitingVolumeDetachmentEvent(n, blockingVAs...))
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}

	stored := nc.DeepCopy()
	_ = nc.StatusConditions().SetTrue(v1.ConditionTypeVolumesDetached)
	if err := c.kubeClient.Status().Patch(ctx, nc, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		if errors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, err
	}
	if err := terminationreconcile.RemoveFinalizer(ctx, c.kubeClient, n, c.Finalizer()); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (c *Controller) blockingVolumeAttachments(ctx context.Context, n *corev1.Node) ([]*storagev1.VolumeAttachment, error) {
	vas, err := nodeutils.GetVolumeAttachments(ctx, c.kubeClient, n)
	if err != nil {
		return nil, err
	}
	if len(vas) == 0 {
		return nil, nil
	}

	pods, err := nodeutils.GetPods(ctx, c.kubeClient, n)
	if err != nil {
		return nil, err
	}
	pods = lo.Reject(pods, func(p *corev1.Pod, _ int) bool {
		return podutils.IsDrainable(p, c.clk)
	})

	// Determine the VolumeAttachments associated with non-drainable pods. We consider these non-blocking since they
	// will never be detached without intervention (since the pods aren't drained).
	nonBlockingVolumes := sets.New[string]()
	for _, p := range pods {
		for _, vol := range p.Spec.Volumes {
			pvc, err := volumeutil.GetPersistentVolumeClaim(ctx, c.kubeClient, p, vol)
			if errors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if pvc != nil {
				nonBlockingVolumes.Insert(pvc.Spec.VolumeName)
			}
		}
	}
	blockingVAs := lo.Reject(vas, func(v *storagev1.VolumeAttachment, _ int) bool {
		pvName := v.Spec.Source.PersistentVolumeName
		return pvName == nil || nonBlockingVolumes.Has(*pvName)
	})
	return blockingVAs, nil
}

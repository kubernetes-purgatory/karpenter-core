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

package drain

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/eviction"
	terminationreconcile "sigs.k8s.io/karpenter/pkg/controllers/node/termination/reconcile"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	"sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

type nodeDrainError struct {
	error
}

func newNodeDrainError(err error) *nodeDrainError {
	return &nodeDrainError{error: err}
}

func isNodeDrainError(err error) bool {
	if err == nil {
		return false
	}
	var nodeDrainErr *nodeDrainError
	return errors.As(err, &nodeDrainErr)
}

type Controller struct {
	clock         clock.Clock
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	recorder      events.Recorder
	evictionQueue *eviction.Queue
}

func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, recorder events.Recorder, queue *eviction.Queue) *Controller {
	return &Controller{
		clock:         clk,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		recorder:      recorder,
		evictionQueue: queue,
	}
}

func (*Controller) Name() string {
	return "node.drain"
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
		Complete(terminationreconcile.AsReconciler(m.GetClient(), c.cloudProvider, c.clock, c))
}

func (*Controller) AwaitFinalizers() []string {
	return nil
}

func (*Controller) Finalizer() string {
	return v1.DrainFinalizer
}

func (*Controller) NodeClaimNotFoundPolicy() terminationreconcile.Policy {
	return terminationreconcile.PolicyFinalize
}

func (*Controller) TerminationGracePeriodPolicy() terminationreconcile.Policy {
	return terminationreconcile.PolicyFinalize
}

// nolint:gocyclo
func (c *Controller) Reconcile(ctx context.Context, n *corev1.Node, nc *v1.NodeClaim) (reconcile.Result, error) {
	if err := c.prepareNode(ctx, n); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		if apierrors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, fmt.Errorf("tainting node with %s, %w", pretty.Taint(v1.DisruptedNoScheduleTaint), err)
	}

	tgpExpirationTime, err := nodeclaim.TerminationGracePeriodExpirationTime(nc)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to terminate node")
		return reconcile.Result{}, nil
	}
	if tgpExpirationTime != nil {
		c.recorder.Publish(NodeTerminationGracePeriodExpiringEvent(n, *tgpExpirationTime))
	}

	if err := c.drain(ctx, n, tgpExpirationTime); err != nil {
		if !isNodeDrainError(err) {
			return reconcile.Result{}, fmt.Errorf("draining node, %w", err)
		}
		stored := nc.DeepCopy()
		if nc.StatusConditions().SetFalse(v1.ConditionTypeDrained, "Draining", "Draining") {
			if err := c.kubeClient.Status().Patch(ctx, nc, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
				if apierrors.IsNotFound(err) {
					return reconcile.Result{}, nil
				}
				if apierrors.IsConflict(err) {
					return reconcile.Result{Requeue: true}, nil
				}
				return reconcile.Result{}, err
			}
		}
		c.recorder.Publish(NodeDrainFailedEvent(n, err))
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}

	storedNodeClaim := nc.DeepCopy()
	_ = nc.StatusConditions().SetTrue(v1.ConditionTypeDrained)
	if err := c.kubeClient.Status().Patch(ctx, nc, client.MergeFromWithOptions(storedNodeClaim, client.MergeFromWithOptimisticLock{})); client.IgnoreNotFound(err) != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		if apierrors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, err
	}
	if err := terminationreconcile.RemoveFinalizer(ctx, c.kubeClient, n, c.Finalizer()); err != nil {
		return reconcile.Result{}, err
	}
	NodesDrainedTotal.Inc(map[string]string{
		metrics.NodePoolLabel: n.Labels[v1.NodePoolLabelKey],
	})
	return reconcile.Result{}, nil
}

// prepareNode ensures that the node is ready to begin the drain / termination process. This includes ensuring that it
// is tainted appropriately and annotated to be ignored by external load balancers.
func (c *Controller) prepareNode(ctx context.Context, n *corev1.Node) error {
	stored := n.DeepCopy()

	// Add the karpenter.sh/disrupted:NoSchedule taint to ensure no additional pods schedule to the Node during the
	// drain process.
	if !lo.ContainsBy(n.Spec.Taints, func(t corev1.Taint) bool {
		return t.MatchTaint(&v1.DisruptedNoScheduleTaint)
	}) {
		n.Spec.Taints = append(n.Spec.Taints, v1.DisruptedNoScheduleTaint)
	}
	// Adding this label to the node ensures that the node is removed from the load-balancer target group while it is
	// draining and before it is terminated. This prevents 500s coming prior to health check when the load balancer
	// controller hasn't yet determined that the node and underlying connections are gone.
	// https://github.com/aws/aws-node-termination-handler/issues/316
	// https://github.com/aws/karpenter/pull/2518
	n.Labels = lo.Assign(n.Labels, map[string]string{
		corev1.LabelNodeExcludeBalancers: "karpenter",
	})

	if equality.Semantic.DeepEqual(n, stored) {
		return nil
	}
	// We use client.MergeFromWithOptimisticLock because patching a list with a JSON merge patch can cause races due
	// to the fact that it fully replaces the list on a change. Here, we are updating the taint list.
	return c.kubeClient.Patch(ctx, n, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

// Drain evicts pods from the node and returns true when all pods are evicted
// https://kubernetes.io/docs/concepts/architecture/nodes/#graceful-node-shutdown
func (c *Controller) drain(ctx context.Context, node *corev1.Node, nodeGracePeriodExpirationTime *time.Time) error {
	pods, err := nodeutils.GetPods(ctx, c.kubeClient, node)
	if err != nil {
		return fmt.Errorf("listing pods on node, %w", err)
	}
	podsToDelete := lo.Filter(pods, func(p *corev1.Pod, _ int) bool {
		return podutils.IsWaitingEviction(p, c.clock) && !podutils.IsTerminating(p)
	})
	if err := c.DeleteExpiringPods(ctx, podsToDelete, nodeGracePeriodExpirationTime); err != nil {
		return fmt.Errorf("deleting expiring pods, %w", err)
	}
	// Monitor pods in pod groups that either haven't been evicted or are actively evicting
	podGroups := c.groupPodsByPriority(lo.Filter(pods, func(p *corev1.Pod, _ int) bool { return podutils.IsWaitingEviction(p, c.clock) }))
	for _, group := range podGroups {
		if len(group) > 0 {
			// Only add pods to the eviction queue that haven't been evicted yet
			c.evictionQueue.Add(lo.Filter(group, func(p *corev1.Pod, _ int) bool { return podutils.IsEvictable(p) })...)
			return newNodeDrainError(fmt.Errorf("%d pods are waiting to be evicted", lo.SumBy(podGroups, func(pods []*corev1.Pod) int { return len(pods) })))
		}
	}
	return nil
}

func (c *Controller) groupPodsByPriority(pods []*corev1.Pod) [][]*corev1.Pod {
	// 1. Prioritize noncritical pods, non-daemon pods https://kubernetes.io/docs/concepts/architecture/nodes/#graceful-node-shutdown
	var nonCriticalNonDaemon, nonCriticalDaemon, criticalNonDaemon, criticalDaemon []*corev1.Pod
	for _, pod := range pods {
		if pod.Spec.PriorityClassName == "system-cluster-critical" || pod.Spec.PriorityClassName == "system-node-critical" {
			if podutils.IsOwnedByDaemonSet(pod) {
				criticalDaemon = append(criticalDaemon, pod)
			} else {
				criticalNonDaemon = append(criticalNonDaemon, pod)
			}
		} else {
			if podutils.IsOwnedByDaemonSet(pod) {
				nonCriticalDaemon = append(nonCriticalDaemon, pod)
			} else {
				nonCriticalNonDaemon = append(nonCriticalNonDaemon, pod)
			}
		}
	}
	return [][]*corev1.Pod{nonCriticalNonDaemon, nonCriticalDaemon, criticalNonDaemon, criticalDaemon}
}

func (c *Controller) DeleteExpiringPods(ctx context.Context, pods []*corev1.Pod, nodeGracePeriodTerminationTime *time.Time) error {
	for _, pod := range pods {
		// check if the node has an expiration time and the pod needs to be deleted
		deleteTime := c.podDeleteTimeWithGracePeriod(nodeGracePeriodTerminationTime, pod)
		if deleteTime != nil && time.Now().After(*deleteTime) {
			// delete pod proactively to give as much of its terminationGracePeriodSeconds as possible for deletion
			// ensure that we clamp the maximum pod terminationGracePeriodSeconds to the node's remaining expiration time in the delete command
			gracePeriodSeconds := lo.ToPtr(int64(time.Until(*nodeGracePeriodTerminationTime).Seconds()))
			c.recorder.Publish(PodDeletedEvent(pod, gracePeriodSeconds, nodeGracePeriodTerminationTime))
			opts := &client.DeleteOptions{
				GracePeriodSeconds: gracePeriodSeconds,
			}
			if err := c.kubeClient.Delete(ctx, pod, opts); err != nil && !apierrors.IsNotFound(err) { // ignore 404, not a problem
				return fmt.Errorf("deleting pod, %w", err) // otherwise, bubble up the error
			}
			log.FromContext(ctx).WithValues(
				"namespace", pod.Namespace,
				"name", pod.Name,
				"pod.terminationGracePeriodSeconds", *pod.Spec.TerminationGracePeriodSeconds,
				"delete.gracePeriodSeconds", *gracePeriodSeconds,
				"nodeclaim.terminationTime", *nodeGracePeriodTerminationTime,
			).V(1).Info("deleting pod")
		}
	}
	return nil
}

// if a pod should be deleted to give it the full terminationGracePeriodSeconds of time before the node will shut down, return the time the pod should be deleted
func (*Controller) podDeleteTimeWithGracePeriod(nodeGracePeriodExpirationTime *time.Time, pod *corev1.Pod) *time.Time {
	if nodeGracePeriodExpirationTime == nil || pod.Spec.TerminationGracePeriodSeconds == nil { // k8s defaults to 30s, so we should never see a nil TerminationGracePeriodSeconds
		return nil
	}

	// calculate the time the pod should be deleted to allow it's full grace period for termination, equal to its terminationGracePeriodSeconds before the node's expiration time
	// eg: if a node will be force terminated in 30m, but the current pod has a grace period of 45m, we return a time of 15m ago
	deleteTime := nodeGracePeriodExpirationTime.Add(time.Duration(*pod.Spec.TerminationGracePeriodSeconds) * time.Second * -1)
	return &deleteTime
}

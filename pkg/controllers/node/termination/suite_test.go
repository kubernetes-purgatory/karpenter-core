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

package termination_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var terminationController *termination.Controller
var env *test.Environment
var defaultOwnerRefs = []metav1.OwnerReference{{Kind: "ReplicaSet", APIVersion: "appsv1", Name: "rs", UID: "1234567890"}}
var fakeClock *clock.FakeClock
var cloudProvider *fake.CloudProvider
var recorder *test.EventRecorder
var queue *terminator.Queue

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Termination")
}

var _ = BeforeSuite(func() {
	fakeClock = clock.NewFakeClock(time.Now())
	env = test.NewEnvironment(
		test.WithCRDs(apis.CRDs...),
		test.WithCRDs(v1alpha1.CRDs...),
		test.WithFieldIndexers(test.NodeClaimProviderIDFieldIndexer(ctx), test.VolumeAttachmentFieldIndexer(ctx)),
	)

	cloudProvider = fake.NewCloudProvider()
	recorder = test.NewEventRecorder()
	queue = terminator.NewTestingQueue(env.Client, recorder)
	terminationController = termination.NewController(fakeClock, env.Client, cloudProvider, terminator.NewTerminator(fakeClock, env.Client, queue, recorder), recorder)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Termination", func() {
	var node *corev1.Node
	var nodeClaim *v1.NodeClaim
	var nodePool *v1.NodePool

	BeforeEach(func() {
		fakeClock.SetTime(time.Now())
		cloudProvider.Reset()
		*queue = lo.FromPtr(terminator.NewTestingQueue(env.Client, recorder))

		nodePool = test.NodePool()
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Finalizers: []string{v1.TerminationFinalizer}}})
		node.Labels[v1.NodePoolLabelKey] = test.NodePool().Name
		cloudProvider.CreatedNodeClaims[node.Spec.ProviderID] = nodeClaim
	})

	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)

		// Reset the metrics collectors
		metrics.NodesTerminatedTotal.Reset()
		termination.DurationSeconds.Reset()
		termination.NodeLifetimeDurationSeconds.Reset()
		termination.NodesDrainedTotal.Reset()
	})

	Context("Reconciliation", func() {
		It("should delete nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should ignore nodes not managed by this Karpenter instance", func() {
			delete(node.Labels, "karpenter.test.sh/testnodeclass")
			node.Labels = lo.Assign(node.Labels, map[string]string{"karpenter.test.sh/unmanagednodeclass": "default"})
			ExpectApplied(ctx, env.Client, node)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)

			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectExists(ctx, env.Client, node)
		})
		It("should delete nodeclaims associated with nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)

			// The first reconciliation should trigger the Delete, and set the terminating status condition
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			nc := ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nc.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue()).To(BeTrue())
			ExpectNodeExists(ctx, env.Client, node.Name)

			// The second reconciliation should call get, see the "instance" is terminated, and remove the node.
			// We should have deleted the NodeClaim from the node termination controller, so removing it's finalizer should result in it being removed.
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectFinalizersRemoved(ctx, env.Client, nodeClaim)
			ExpectNotFound(ctx, env.Client, node, nodeClaim)
		})
		It("should not race if deleting nodes in parallel", func() {
			nodes := lo.Times(10, func(_ int) *corev1.Node {
				return test.NodeClaimLinkedNode(nodeClaim)
			})
			for _, node := range nodes {
				ExpectApplied(ctx, env.Client, node, nodeClaim)
				Expect(env.Client.Delete(ctx, node)).To(Succeed())
				*node = *ExpectNodeExists(ctx, env.Client, node.Name)
			}

			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			for range 2 {
				var wg sync.WaitGroup
				// this is enough to trip the race detector
				for i := range nodes {
					wg.Add(1)
					go func(node *corev1.Node) {
						defer GinkgoRecover()
						defer wg.Done()
						ExpectObjectReconciled(ctx, env.Client, terminationController, node)
					}(nodes[i])
				}
				wg.Wait()
			}
			for _, node := range nodes {
				ExpectNotFound(ctx, env.Client, node)
			}
		})
		It("should exclude nodes from load balancers when terminating", func() {
			labels := map[string]string{"foo": "bar"}
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: defaultOwnerRefs,
					Labels:          labels,
				},
			})
			// Create a fully blocking PDB to prevent the node from being deleted before we can observe its labels
			pdb := test.PodDisruptionBudget(test.PDBOptions{
				Labels:         labels,
				MaxUnavailable: lo.ToPtr(intstr.FromInt(0)),
			})

			ExpectApplied(ctx, env.Client, node, nodeClaim, pod, pdb)
			ExpectManualBinding(ctx, env.Client, pod, node)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Labels[corev1.LabelNodeExcludeBalancers]).Should(Equal("karpenter"))
		})
		It("should not evict pods that tolerate karpenter disruption taint with equal operator", func() {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podSkip := test.Pod(test.PodOptions{
				NodeName:    node.Name,
				Tolerations: []corev1.Toleration{{Key: v1.DisruptedTaintKey, Operator: corev1.TolerationOpEqual, Effect: v1.DisruptedNoScheduleTaint.Effect, Value: v1.DisruptedNoScheduleTaint.Value}},
				ObjectMeta:  metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs},
			})
			ExpectApplied(ctx, env.Client, node, nodeClaim, podEvict, podSkip)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			Expect(queue.Has(node, podSkip)).To(BeFalse())
			ExpectSingletonReconciled(ctx, queue)

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not evict pods that tolerate karpenter disruption taint with exists operator", func() {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podSkip := test.Pod(test.PodOptions{
				NodeName:    node.Name,
				Tolerations: []corev1.Toleration{{Key: v1.DisruptedTaintKey, Operator: corev1.TolerationOpExists, Effect: v1.DisruptedNoScheduleTaint.Effect}},
				ObjectMeta:  metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs},
			})
			ExpectApplied(ctx, env.Client, node, nodeClaim, podEvict, podSkip)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			Expect(queue.Has(node, podSkip)).To(BeFalse())
			ExpectSingletonReconciled(ctx, queue)

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			Expect(queue.Has(node, podSkip)).To(BeFalse())

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should evict pods that tolerate the node.kubernetes.io/unschedulable taint", func() {
			podEvict := test.Pod(test.PodOptions{
				NodeName:    node.Name,
				Tolerations: []corev1.Toleration{{Key: corev1.TaintNodeUnschedulable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
				ObjectMeta:  metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs},
			})
			ExpectApplied(ctx, env.Client, node, nodeClaim, podEvict)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete nodes that have pods without an ownerRef", func() {
			pod := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: nil,
				},
			})

			ExpectApplied(ctx, env.Client, node, nodeClaim, pod)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)

			// Expect pod with no owner ref to be enqueued for eviction
			EventuallyExpectTerminating(ctx, env.Client, pod)

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Delete no owner refs pod to simulate successful eviction
			ExpectDeleted(ctx, env.Client, pod)

			// Reconcile node to evict pod and delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should delete nodes with terminal pods", func() {
			podEvictPhaseSucceeded := test.Pod(test.PodOptions{
				NodeName: node.Name,
				Phase:    corev1.PodSucceeded,
			})
			podEvictPhaseFailed := test.Pod(test.PodOptions{
				NodeName: node.Name,
				Phase:    corev1.PodFailed,
			})

			ExpectApplied(ctx, env.Client, node, nodeClaim, podEvictPhaseSucceeded, podEvictPhaseFailed)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Trigger Termination Controller, which should ignore these pods and delete the node
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should fail to evict pods that violate a PDB", func() {
			minAvailable := intstr.FromInt32(1)
			labelSelector := map[string]string{test.RandomName(): test.RandomName()}
			pdb := test.PodDisruptionBudget(test.PDBOptions{
				Labels: labelSelector,
				// Don't let any pod evict
				MinAvailable: &minAvailable,
			})
			podNoEvict := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Labels:          labelSelector,
					OwnerReferences: defaultOwnerRefs,
				},
				Phase: corev1.PodRunning,
			})

			ExpectApplied(ctx, env.Client, node, nodeClaim, podNoEvict, pdb)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podNoEvict to be added to the queue
			Expect(queue.Has(node, podNoEvict)).To(BeTrue())

			// Attempt to evict the pod, but fail to do so
			ExpectSingletonReconciled(ctx, queue)

			// Expect podNoEvict to fail eviction due to PDB, and be retried
			Expect(queue.Has(node, podNoEvict)).To(BeTrue())

			// Delete pod to simulate successful eviction
			ExpectDeleted(ctx, env.Client, podNoEvict)
			ExpectNotFound(ctx, env.Client, podNoEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should evict pods in order and wait until pods are fully deleted", func() {
			daemonEvict := test.DaemonSet()
			daemonNodeCritical := test.DaemonSet(test.DaemonSetOptions{PodOptions: test.PodOptions{PriorityClassName: "system-node-critical"}})
			daemonClusterCritical := test.DaemonSet(test.DaemonSetOptions{PodOptions: test.PodOptions{PriorityClassName: "system-cluster-critical"}})
			ExpectApplied(ctx, env.Client, daemonEvict, daemonNodeCritical, daemonClusterCritical)

			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podDaemonEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "DaemonSet",
				Name:               daemonEvict.Name,
				UID:                daemonEvict.UID,
				Controller:         lo.ToPtr(true),
				BlockOwnerDeletion: lo.ToPtr(true),
			}}}})
			podNodeCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-node-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podClusterCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-cluster-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podDaemonNodeCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-node-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "DaemonSet",
				Name:               daemonNodeCritical.Name,
				UID:                daemonNodeCritical.UID,
				Controller:         lo.ToPtr(true),
				BlockOwnerDeletion: lo.ToPtr(true),
			}}}})
			podDaemonClusterCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-cluster-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "DaemonSet",
				Name:               daemonClusterCritical.Name,
				UID:                daemonClusterCritical.UID,
				Controller:         lo.ToPtr(true),
				BlockOwnerDeletion: lo.ToPtr(true),
			}}}})

			ExpectApplied(ctx, env.Client, node, nodeClaim, podEvict, podNodeCritical, podClusterCritical, podDaemonEvict, podDaemonNodeCritical, podDaemonClusterCritical)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())

			podGroups := [][]*corev1.Pod{{podEvict}, {podDaemonEvict}, {podNodeCritical, podClusterCritical}, {podDaemonNodeCritical, podDaemonClusterCritical}}
			for i, podGroup := range podGroups {
				node = ExpectNodeExists(ctx, env.Client, node.Name)
				for _, p := range podGroup {
					ExpectPodExists(ctx, env.Client, p.Name, p.Namespace)
				}
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
				for range podGroup {
					ExpectSingletonReconciled(ctx, queue)
				}
				// Start draining the pod group, but don't complete it yet
				EventuallyExpectTerminating(ctx, env.Client, lo.Map(podGroup, func(p *corev1.Pod, _ int) client.Object { return p })...)

				// Look at the next pod group and ensure that none of the pods have started terminating on it
				if i != len(podGroups)-1 {
					for range podGroups[i+1] {
						ExpectSingletonReconciled(ctx, queue)
					}
					ConsistentlyExpectNotTerminating(ctx, env.Client, lo.Map(podGroups[i+1], func(p *corev1.Pod, _ int) client.Object { return p })...)
				}
				// Expect that the pods are deleted -- which should unblock the next pod group
				ExpectDeleted(ctx, env.Client, lo.Map(podGroup, func(p *corev1.Pod, _ int) client.Object { return p })...)
			}

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should evict non-critical pods first", func() {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podNodeCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-node-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podClusterCritical := test.Pod(test.PodOptions{NodeName: node.Name, PriorityClassName: "system-cluster-critical", ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})

			ExpectApplied(ctx, env.Client, node, nodeClaim, podEvict, podNodeCritical, podClusterCritical)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Expect podEvict to be evicting, and delete it
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			// Expect the critical pods to be evicted and deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)
			ExpectSingletonReconciled(ctx, queue)

			EventuallyExpectTerminating(ctx, env.Client, podNodeCritical, podClusterCritical)
			ExpectDeleted(ctx, env.Client, podNodeCritical)
			ExpectDeleted(ctx, env.Client, podClusterCritical)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not evict static pods", func() {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			ExpectApplied(ctx, env.Client, node, nodeClaim, podEvict)

			podNoEvict := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "v1",
						Kind:       "Node",
						Name:       node.Name,
						UID:        node.UID,
					}},
				},
			})
			ExpectApplied(ctx, env.Client, podNoEvict)

			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)

			// Expect mirror pod to not be queued for eviction
			Expect(queue.Has(node, podNoEvict)).To(BeFalse())

			// Expect podEvict to be enqueued for eviction then be successful
			EventuallyExpectTerminating(ctx, env.Client, podEvict)

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Reconcile node to evict pod
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)

			// Delete pod to simulate successful eviction
			ExpectDeleted(ctx, env.Client, podEvict)

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not delete nodes until all pods are deleted", func() {
			pods := test.Pods(2, test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			ExpectApplied(ctx, env.Client, node, nodeClaim, pods[0], pods[1])
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)
			ExpectSingletonReconciled(ctx, queue)

			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).Reason).To(Equal("Draining"))

			// Expect the pods to be evicted
			EventuallyExpectTerminating(ctx, env.Client, pods[0], pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			ExpectDeleted(ctx, env.Client, pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			ExpectDeleted(ctx, env.Client, pods[0])

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
		})
		It("should delete nodes with no underlying instance even if not fully drained", func() {
			pods := test.Pods(2, test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			ExpectApplied(ctx, env.Client, node, nodeClaim, pods[0], pods[1])

			// Make Node NotReady since it's automatically marked as Ready on first deploy
			ExpectMakeNodesNotReady(ctx, env.Client, node)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)
			ExpectSingletonReconciled(ctx, queue)

			// Expect the pods to be evicted
			EventuallyExpectTerminating(ctx, env.Client, pods[0], pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// After this, the node still has one pod that is evicting.
			ExpectDeleted(ctx, env.Client, pods[1])

			// Remove the node from created nodeclaims so that the cloud provider returns DNE
			cloudProvider.CreatedNodeClaims = map[string]*v1.NodeClaim{}

			// Reconcile to delete node
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not delete nodes with no underlying instance if the node is still Ready", func() {
			pods := test.Pods(2, test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			ExpectApplied(ctx, env.Client, node, nodeClaim, pods[0], pods[1])

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)
			ExpectSingletonReconciled(ctx, queue)

			// Expect the pods to be evicted
			EventuallyExpectTerminating(ctx, env.Client, pods[0], pods[1])

			// Expect node to exist and be draining, but not deleted
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// After this, the node still has one pod that is evicting.
			ExpectDeleted(ctx, env.Client, pods[1])

			// Remove the node from created nodeclaims so that the cloud provider returns DNE
			cloudProvider.CreatedNodeClaims = map[string]*v1.NodeClaim{}

			// Reconcile to try to delete the node, but don't succeed because the readiness condition
			// of the node still won't let us delete it
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectExists(ctx, env.Client, node)
		})
		It("should wait for pods to terminate", func() {
			pod := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			fakeClock.SetTime(time.Now()) // make our fake clock match the pod creation time
			ExpectApplied(ctx, env.Client, node, nodeClaim, pod)

			// Before grace period, node should not delete
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)
			ExpectNodeExists(ctx, env.Client, node.Name)
			EventuallyExpectTerminating(ctx, env.Client, pod)

			// After grace period, node should delete. The deletion timestamps are from etcd which we can't control, so
			// to eliminate test-flakiness we reset the time to current time + 90 seconds instead of just advancing
			// the clock by 90 seconds.
			fakeClock.SetTime(time.Now().Add(90 * time.Second))
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNotFound(ctx, env.Client, node)
		})
		It("should not evict a new pod with the same name using the old pod's eviction queue key", func() {
			pod := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-pod",
					OwnerReferences: defaultOwnerRefs,
				},
			})
			ExpectApplied(ctx, env.Client, node, nodeClaim, pod)

			// Trigger Termination Controller
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)

			// Don't trigger a call into the queue to make sure that we effectively aren't triggering eviction
			// We'll use this to try to leave pods in the queue

			// Expect node to exist and be draining
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

			// Delete the pod directly to act like something else is doing the pod termination
			ExpectDeleted(ctx, env.Client, pod)

			// Requeue the termination controller to completely delete the node
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)

			// Expect that the old pod's key still exists in the queue
			Expect(queue.Has(node, pod)).To(BeTrue())

			// Re-create the pod and node, it should now have the same name, but a different UUID
			node = test.Node(test.NodeOptions{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{v1.TerminationFinalizer},
				},
			})
			pod = test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-pod",
					OwnerReferences: defaultOwnerRefs,
				},
			})
			ExpectApplied(ctx, env.Client, node, pod)

			// Trigger eviction queue with the pod key still in it
			ExpectSingletonReconciled(ctx, queue)

			Consistently(func(g Gomega) {
				g.Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
				g.Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
			}, ReconcilerPropagationTime, RequestInterval).Should(Succeed())
		})
		It("should preemptively delete pods to satisfy their terminationGracePeriodSeconds", func() {
			nodeClaim.Spec.TerminationGracePeriod = &metav1.Duration{Duration: time.Second * 300}
			nodeClaim.Annotations = map[string]string{
				v1.NodeClaimTerminationTimestampAnnotationKey: time.Now().Add(nodeClaim.Spec.TerminationGracePeriod.Duration).Format(time.RFC3339),
			}
			pod := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						v1.DoNotDisruptAnnotationKey: "true",
					},
					OwnerReferences: defaultOwnerRefs,
				},
				TerminationGracePeriodSeconds: lo.ToPtr(int64(300)),
			})
			ExpectApplied(ctx, env.Client, node, nodeClaim, nodePool, pod)

			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)
			ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectDeleted(ctx, env.Client, pod)
		})
		It("should only delete pods when their terminationGracePeriodSeconds is less than the the node's remaining terminationGracePeriod", func() {
			nodeClaim.Spec.TerminationGracePeriod = &metav1.Duration{Duration: time.Second * 300}
			nodeClaim.Annotations = map[string]string{
				v1.NodeClaimTerminationTimestampAnnotationKey: time.Now().Add(nodeClaim.Spec.TerminationGracePeriod.Duration).Format(time.RFC3339),
			}
			pod := test.Pod(test.PodOptions{
				NodeName: node.Name,
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						v1.DoNotDisruptAnnotationKey: "true",
					},
					OwnerReferences: defaultOwnerRefs,
				},
				TerminationGracePeriodSeconds: lo.ToPtr(int64(60)),
			})
			fakeClock.SetTime(time.Now())
			ExpectApplied(ctx, env.Client, node, nodeClaim, nodePool, pod)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())

			// expect pod still exists
			fakeClock.Step(90 * time.Second)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
			ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectPodExists(ctx, env.Client, pod.Name, pod.Namespace)

			// expect pod is now deleted
			fakeClock.Step(175 * time.Second)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectSingletonReconciled(ctx, queue)
			ExpectDeleted(ctx, env.Client, pod)
		})
		Context("VolumeAttachments", func() {
			It("should wait for volume attachments", func() {
				va := test.VolumeAttachment(test.VolumeAttachmentOptions{
					NodeName:   node.Name,
					VolumeName: "foo",
				})
				ExpectApplied(ctx, env.Client, node, nodeClaim, nodePool, va)
				Expect(env.Client.Delete(ctx, node)).To(Succeed())
				ExpectExists(ctx, env.Client, nodeClaim)
				Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsUnknown()).To(BeTrue())

				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectExists(ctx, env.Client, node)
				nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
				Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsFalse()).To(BeTrue())
				Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).Reason).To(Equal("AwaitingVolumeDetachment"))

				ExpectDeleted(ctx, env.Client, va)
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectNotFound(ctx, env.Client, node)

				nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
				Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsTrue()).To(BeTrue())
			})
			It("should only wait for volume attachments associated with drainable pods", func() {
				vaDrainable := test.VolumeAttachment(test.VolumeAttachmentOptions{
					NodeName:   node.Name,
					VolumeName: "foo",
				})
				vaNonDrainable := test.VolumeAttachment(test.VolumeAttachmentOptions{
					NodeName:   node.Name,
					VolumeName: "bar",
				})
				pvc := test.PersistentVolumeClaim(test.PersistentVolumeClaimOptions{
					VolumeName: "bar",
				})
				pod := test.Pod(test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{
						OwnerReferences: defaultOwnerRefs,
					},
					Tolerations: []corev1.Toleration{{
						Key:      v1.DisruptedTaintKey,
						Operator: corev1.TolerationOpExists,
					}},
					PersistentVolumeClaims: []string{pvc.Name},
				})
				ExpectApplied(ctx, env.Client, node, nodeClaim, nodePool, vaDrainable, vaNonDrainable, pod, pvc)
				ExpectManualBinding(ctx, env.Client, pod, node)
				Expect(env.Client.Delete(ctx, node)).To(Succeed())

				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectExists(ctx, env.Client, node)

				ExpectDeleted(ctx, env.Client, vaDrainable)
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectNotFound(ctx, env.Client, node)
			})
			It("should wait for volume attachments until the nodeclaim's termination grace period expires", func() {
				va := test.VolumeAttachment(test.VolumeAttachmentOptions{
					NodeName:   node.Name,
					VolumeName: "foo",
				})
				nodeClaim.Annotations = map[string]string{
					v1.NodeClaimTerminationTimestampAnnotationKey: fakeClock.Now().Add(time.Minute).Format(time.RFC3339),
				}
				ExpectApplied(ctx, env.Client, node, nodeClaim, nodePool, va)
				Expect(env.Client.Delete(ctx, node)).To(Succeed())

				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectExists(ctx, env.Client, node)

				fakeClock.Step(5 * time.Minute)
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectObjectReconciled(ctx, env.Client, terminationController, node)
				ExpectNotFound(ctx, env.Client, node)
			})
		})
	})
	Context("Metrics", func() {
		It("should fire the terminationSummary metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)

			m, ok := FindMetricWithLabelValues("karpenter_nodes_termination_duration_seconds", map[string]string{"nodepool": node.Labels[v1.NodePoolLabelKey]})
			Expect(ok).To(BeTrue())
			Expect(m.GetSummary().GetSampleCount()).To(BeNumerically("==", 1))
		})
		It("should fire the nodesTerminated counter metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectMetricCounterValue(termination.NodesDrainedTotal, 1, map[string]string{"nodepool": node.Labels[v1.NodePoolLabelKey]})
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)

			m, ok := FindMetricWithLabelValues("karpenter_nodes_terminated_total", map[string]string{"nodepool": node.Labels[v1.NodePoolLabelKey]})
			Expect(ok).To(BeTrue())
			Expect(lo.FromPtr(m.GetCounter().Value)).To(BeNumerically("==", 1))
		})
		It("should fire the lifetime duration histogram metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)

			m, ok := FindMetricWithLabelValues("karpenter_nodes_lifetime_duration_seconds", map[string]string{"nodepool": node.Labels[v1.NodePoolLabelKey]})
			Expect(ok).To(BeTrue())
			Expect(lo.FromPtr(m.GetHistogram().SampleCount)).To(BeNumerically("==", 1))
		})
		It("should update the eviction queueDepth metric when reconciling pods", func() {
			minAvailable := intstr.FromInt32(0)
			labelSelector := map[string]string{test.RandomName(): test.RandomName()}
			pdb := test.PodDisruptionBudget(test.PDBOptions{
				Labels: labelSelector,
				// Don't let any pod evict
				MinAvailable: &minAvailable,
			})
			ExpectApplied(ctx, env.Client, pdb, node, nodeClaim)
			pods := test.Pods(5, test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: defaultOwnerRefs,
				Labels:          labelSelector,
			}})
			ExpectApplied(ctx, env.Client, lo.Map(pods, func(p *corev1.Pod, _ int) client.Object { return p })...)

			wqDepthBefore, _ := FindMetricWithLabelValues("workqueue_adds_total", map[string]string{"name": "eviction.workqueue"})
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			ExpectObjectReconciled(ctx, env.Client, terminationController, node)
			wqDepthAfter, ok := FindMetricWithLabelValues("workqueue_adds_total", map[string]string{"name": "eviction.workqueue"})
			Expect(ok).To(BeTrue())
			Expect(lo.FromPtr(wqDepthAfter.GetCounter().Value) - lo.FromPtr(wqDepthBefore.GetCounter().Value)).To(BeNumerically("==", 5))
		})
	})
})

func ExpectNodeWithNodeClaimDraining(c client.Client, nodeName string) *corev1.Node {
	GinkgoHelper()
	node := ExpectNodeExists(ctx, c, nodeName)
	Expect(node.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
	Expect(lo.Contains(node.Finalizers, v1.TerminationFinalizer)).To(BeTrue())
	Expect(node.DeletionTimestamp).ToNot(BeNil())
	return node
}

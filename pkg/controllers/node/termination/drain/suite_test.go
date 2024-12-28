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

package drain_test

import (
	"context"
	"testing"
	"time"

	clock "k8s.io/utils/clock/testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/drain"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/eviction"
	terminationreconcile "sigs.k8s.io/karpenter/pkg/controllers/node/termination/reconcile"
	"sigs.k8s.io/karpenter/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var queue *eviction.Queue
var env *test.Environment
var fakeClock *clock.FakeClock
var cloudProvider *fake.CloudProvider
var recorder *test.EventRecorder

var reconciler reconcile.Reconciler

var defaultOwnerRefs = []metav1.OwnerReference{{Kind: "ReplicaSet", APIVersion: "appsv1", Name: "rs", UID: "1234567890"}}

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Drain")
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
	queue = eviction.NewTestingQueue(env.Client, recorder)
	reconciler = terminationreconcile.AsReconciler(env.Client, cloudProvider, fakeClock, drain.NewController(fakeClock, env.Client, cloudProvider, recorder, queue))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Drain", func() {
	var node *corev1.Node
	var nodeClaim *v1.NodeClaim

	BeforeEach(func() {
		fakeClock.SetTime(time.Now())
		cloudProvider.Reset()
		*queue = lo.FromPtr(eviction.NewTestingQueue(env.Client, recorder))
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Finalizers: []string{
			v1.TerminationFinalizer,
			v1.DrainFinalizer,
		}}})
		cloudProvider.CreatedNodeClaims[node.Spec.ProviderID] = nodeClaim
	})
	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
		drain.NodesDrainedTotal.Reset()
	})

	// TODO: Move to reconciler
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
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		node = ExpectNodeExists(ctx, env.Client, node.Name)
		Expect(node.Labels[corev1.LabelNodeExcludeBalancers]).Should(Equal("karpenter"))
	})
	DescribeTable(
		"should not evict pods that tolerate the 'karpenter.sh/disrupted' taint",
		func(toleration corev1.Toleration) {
			podEvict := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
			podSkip := test.Pod(test.PodOptions{
				NodeName:    node.Name,
				Tolerations: []corev1.Toleration{toleration},
				ObjectMeta:  metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs},
			})
			ExpectApplied(ctx, env.Client, node, nodeClaim, podEvict, podSkip)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())

			Expect(queue.Has(podSkip)).To(BeFalse())
			Expect(queue.Has(podEvict)).To(BeTrue())
			ExpectSingletonReconciled(ctx, queue)
			EventuallyExpectTerminating(ctx, env.Client, podEvict)
			ExpectDeleted(ctx, env.Client, podEvict)

			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
			node = ExpectExists(ctx, env.Client, node)
			Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
		},
		Entry("with an equals operator", corev1.Toleration{Key: v1.DisruptedTaintKey, Operator: corev1.TolerationOpEqual, Effect: v1.DisruptedNoScheduleTaint.Effect, Value: v1.DisruptedNoScheduleTaint.Value}),
		Entry("with an exists operator", corev1.Toleration{Key: v1.DisruptedTaintKey, Operator: corev1.TolerationOpExists, Effect: v1.DisruptedNoScheduleTaint.Effect}),
	)
	It("should evict pods that tolerate the node.kubernetes.io/unschedulable taint", func() {
		podEvict := test.Pod(test.PodOptions{
			NodeName:    node.Name,
			Tolerations: []corev1.Toleration{{Key: corev1.TaintNodeUnschedulable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
			ObjectMeta:  metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs},
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim, podEvict)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())

		Expect(queue.Has(podEvict)).To(BeTrue())
		ExpectSingletonReconciled(ctx, queue)
		EventuallyExpectTerminating(ctx, env.Client, podEvict)
		ExpectDeleted(ctx, env.Client, podEvict)

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
	})
	It("should finalize nodes that have pods without an ownerRef", func() {
		pod := test.Pod(test.PodOptions{
			NodeName: node.Name,
			ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: nil,
			},
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim, pod)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())

		Expect(queue.Has(pod)).To(BeTrue())
		ExpectSingletonReconciled(ctx, queue)
		EventuallyExpectTerminating(ctx, env.Client, pod)
		ExpectDeleted(ctx, env.Client, pod)

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
	})
	It("should finalize nodes with terminal pods", func() {
		podEvictPhaseSucceeded := test.Pod(test.PodOptions{
			NodeName: node.Name,
			Phase:    corev1.PodSucceeded,
		})
		podEvictPhaseFailed := test.Pod(test.PodOptions{
			NodeName: node.Name,
			Phase:    corev1.PodFailed,
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim, podEvictPhaseSucceeded, podEvictPhaseFailed)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())

		Expect(queue.Has(podEvictPhaseSucceeded)).To(BeFalse())
		Expect(queue.Has(podEvictPhaseFailed)).To(BeFalse())

		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
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
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())

		// Expect podNoEvict to be added to the queue - we won't evict it due to PDBs
		Expect(queue.Has(podNoEvict)).To(BeTrue())
		ExpectSingletonReconciled(ctx, queue)
		Expect(queue.Has(podNoEvict)).To(BeTrue())
		ExpectDeleted(ctx, env.Client, podNoEvict)
		ExpectNotFound(ctx, env.Client, podNoEvict)

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
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
		Expect(env.Client.Delete(ctx, node)).To(Succeed())

		podGroups := [][]*corev1.Pod{{podEvict}, {podDaemonEvict}, {podNodeCritical, podClusterCritical}, {podDaemonNodeCritical, podDaemonClusterCritical}}
		for i, podGroup := range podGroups {
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			for _, p := range podGroup {
				ExpectPodExists(ctx, env.Client, p.Name, p.Namespace)
			}
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			if i == 0 {
				Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())
			} else {
				Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())
			}
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
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

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
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
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		node = ExpectNodeExists(ctx, env.Client, node.Name)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())

		// Expect mirror pod to not be queued for eviction
		Expect(queue.Has(podNoEvict)).To(BeFalse())
		Expect(queue.Has(podEvict)).To(BeTrue())
		ExpectSingletonReconciled(ctx, queue)
		EventuallyExpectTerminating(ctx, env.Client, podEvict)
		ExpectDeleted(ctx, env.Client, podEvict)

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
	})
	It("should not finalize nodes until all pods are deleted", func() {
		pods := test.Pods(2, test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
		ExpectApplied(ctx, env.Client, node, nodeClaim, pods[0], pods[1])
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		node = ExpectNodeExists(ctx, env.Client, node.Name)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())

		for _, p := range pods {
			Expect(queue.Has(p)).To(BeTrue())
		}
		ExpectSingletonReconciled(ctx, queue)
		ExpectSingletonReconciled(ctx, queue)
		EventuallyExpectTerminating(ctx, env.Client, pods[0], pods[1])
		ExpectDeleted(ctx, env.Client, pods[1])

		// Expect node to exist and be draining, but not deleted
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)

		ExpectDeleted(ctx, env.Client, pods[0])

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
	})
	It("should finalize nodes when pods are stuck terminating", func() {
		pod := test.Pod(test.PodOptions{NodeName: node.Name, ObjectMeta: metav1.ObjectMeta{OwnerReferences: defaultOwnerRefs}})
		fakeClock.SetTime(time.Now()) // make our fake clock match the pod creation time
		ExpectApplied(ctx, env.Client, node, nodeClaim, pod)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

		// Before grace period, node should not delete
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())

		Expect(queue.Has(pod)).To(BeTrue())
		ExpectSingletonReconciled(ctx, queue)
		EventuallyExpectTerminating(ctx, env.Client, pod)

		// After grace period, node should delete. The deletion timestamps are from etcd which we can't control, so
		// to eliminate test-flakiness we reset the time to current time + 90 seconds instead of just advancing
		// the clock by 90 seconds.
		fakeClock.SetTime(time.Now().Add(90 * time.Second))
		// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
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
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsUnknown()).To(BeTrue())

		// Trigger Termination Controller
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsFalse()).To(BeTrue())

		// Don't trigger a call into the queue to make sure that we effectively aren't triggering eviction
		// We'll use this to try to leave pods in the queue

		// Delete the pod directly to act like something else is doing the pod termination
		ExpectDeleted(ctx, env.Client, pod)

		// Requeue the termination controller to completely delete the node
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeDrained).IsTrue()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.DrainFinalizer))
		ExpectFinalizersRemoved(ctx, env.Client, node)
		ExpectNotFound(ctx, env.Client, node)

		// Expect that the old pod's key still exists in the queue
		Expect(queue.Has(pod)).To(BeTrue())

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
		ExpectApplied(ctx, env.Client, node, nodeClaim, pod)

		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
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
		ExpectApplied(ctx, env.Client, node, nodeClaim, pod)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())

		// expect pod still exists
		fakeClock.Step(90 * time.Second)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNodeWithNodeClaimDraining(env.Client, node.Name)
		ExpectNodeExists(ctx, env.Client, node.Name)
		ExpectPodExists(ctx, env.Client, pod.Name, pod.Namespace)

		// expect pod is now deleted
		fakeClock.Step(175 * time.Second)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectSingletonReconciled(ctx, queue)
		ExpectDeleted(ctx, env.Client, pod)
	})
	Context("Metrics", func() {
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
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
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
	Expect(node.Finalizers).To(ContainElements(v1.TerminationFinalizer, v1.DrainFinalizer))
	Expect(node.DeletionTimestamp).ToNot(BeNil())
	return node
}

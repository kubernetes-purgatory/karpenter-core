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

package reconcile_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	corev1 "k8s.io/api/core/v1"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	terminationreconcile "sigs.k8s.io/karpenter/pkg/controllers/node/termination/reconcile"
	"sigs.k8s.io/karpenter/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var (
	DependentFinalizers = sets.New(v1.DrainFinalizer, v1.VolumeFinalizer)
	TestFinalizer       = "karpenter.sh/test"
)

var ctx context.Context
var env *test.Environment
var fakeClock *clock.FakeClock
var cloudProvider *fake.CloudProvider
var recorder *test.EventRecorder

var testReconciler *TestReconciler
var reconciler reconcile.Reconciler

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "TerminationReconciler")
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
	testReconciler = &TestReconciler{}
	reconciler = terminationreconcile.AsReconciler(env.Client, cloudProvider, fakeClock, testReconciler)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("TerminationReconciler", func() {
	var node *corev1.Node
	var nodeClaim *v1.NodeClaim

	BeforeEach(func() {
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Finalizers: append(DependentFinalizers.UnsortedList(), TestFinalizer),
			},
		})
		testReconciler.Reset()
		cloudProvider.CreatedNodeClaims[node.Spec.ProviderID] = nodeClaim
	})

	It("shouldn't reconcile against Nodes not managed by this instance of Karpenter", func() {
		delete(node.Labels, v1.NodeClassLabelKey(nodeClaim.Spec.NodeClassRef.GroupKind()))
		node.Finalizers = lo.Reject(node.Finalizers, func(finalizer string, _ int) bool {
			return DependentFinalizers.Has(finalizer)
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(TestFinalizer))
		Expect(testReconciler.HasReconciled).To(BeFalse())
	})
	It("shouldn't reconcile against Nodes without deletion timestamps set", func() {
		node.Finalizers = lo.Reject(node.Finalizers, func(finalizer string, _ int) bool {
			return DependentFinalizers.Has(finalizer)
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(TestFinalizer))
		Expect(testReconciler.HasReconciled).To(BeFalse())
	})
	It("shouldn't reconcile against Nodes without the reconciler's finalizer", func() {
		node.Finalizers = lo.Reject(node.Finalizers, func(finalizer string, _ int) bool {
			return finalizer == TestFinalizer
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(testReconciler.HasReconciled).To(BeFalse())
	})
	It("shouldn't reconcile if dependent finalizers are present", func() {
		ExpectApplied(ctx, env.Client, node, nodeClaim)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(TestFinalizer))
		Expect(testReconciler.HasReconciled).To(BeFalse())
	})
	It("shouldn't reconcile if dependent finalizers aren't present, but the Node hasn't been hydrated", func() {
		node.Finalizers = lo.Reject(node.Finalizers, func(finalizer string, _ int) bool {
			return DependentFinalizers.Has(finalizer)
		})
		node.Annotations[v1.HydrationAnnotationKey] = "not-hydrated"
		ExpectApplied(ctx, env.Client, node, nodeClaim)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(TestFinalizer))
		Expect(testReconciler.HasReconciled).To(BeFalse())
	})
	It("shouldn't reconcile against a Node with a duplicate NodeClaim", func() {
		nodeClaim2 := nodeClaim.DeepCopy()
		nodeClaim.Name = "duplicate-nodeclaim"

		node.Finalizers = lo.Reject(node.Finalizers, func(finalizer string, _ int) bool {
			return DependentFinalizers.Has(finalizer)
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim, nodeClaim2)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		// We should still succeed to reconcile - we won't succeed on a subsequent reconciliation so there's no reason
		// to go into error backoff
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeTrue())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(TestFinalizer))
		Expect(testReconciler.HasReconciled).To(BeFalse())
	})
	It("shouldn't reconcile against a Node without a NodeClaim", func() {
		node.Finalizers = lo.Reject(node.Finalizers, func(finalizer string, _ int) bool {
			return DependentFinalizers.Has(finalizer)
		})
		ExpectApplied(ctx, env.Client, node)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(TestFinalizer))
		Expect(testReconciler.HasReconciled).To(BeFalse())
	})
	It("should delete the NodeClaim for a Node", func() {
		node.Finalizers = lo.Reject(node.Finalizers, func(finalizer string, _ int) bool {
			return DependentFinalizers.Has(finalizer)
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeFalse())
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(TestFinalizer))
		Expect(testReconciler.HasReconciled).To(BeTrue())
	})
	DescribeTable(
		"should respect the reconciler's NodeClaimNotFoundPolicy",
		func(policy terminationreconcile.Policy) {
			testReconciler.NodeClaimNotFoundPolicyResult = policy
			delete(cloudProvider.CreatedNodeClaims, node.Spec.ProviderID)
			node.Finalizers = lo.Reject(node.Finalizers, func(finalizer string, _ int) bool {
				return DependentFinalizers.Has(finalizer)
			})
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectMakeNodesNotReady(ctx, env.Client, node)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeFalse())
			switch policy {
			case terminationreconcile.PolicyContinue:
				node = ExpectExists(ctx, env.Client, node)
				Expect(node.Finalizers).To(ContainElement(TestFinalizer))
				Expect(testReconciler.HasReconciled).To(BeTrue())
			case terminationreconcile.PolicyFinalize:
				ExpectNotFound(ctx, env.Client, node)
				Expect(testReconciler.HasReconciled).To(BeFalse())
			}
		},
		Entry("Finalize", terminationreconcile.PolicyFinalize),
		Entry("Continue", terminationreconcile.PolicyContinue),
	)
	DescribeTable(
		"should respect the reconciler's TerminationGracePeriodPolicy",
		func(policy terminationreconcile.Policy) {
			testReconciler.TerminationGracePeriodPolicyResult = policy
			node.Finalizers = lo.Reject(node.Finalizers, func(finalizer string, _ int) bool {
				return DependentFinalizers.Has(finalizer)
			})
			nodeClaim.Annotations[v1.NodeClaimTerminationTimestampAnnotationKey] = fakeClock.Now().Add(-time.Minute).Format(time.RFC3339)
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeFalse())
			switch policy {
			case terminationreconcile.PolicyContinue:
				node = ExpectExists(ctx, env.Client, node)
				Expect(node.Finalizers).To(ContainElement(TestFinalizer))
				Expect(testReconciler.HasReconciled).To(BeTrue())
			case terminationreconcile.PolicyFinalize:
				ExpectNotFound(ctx, env.Client, node)
				Expect(testReconciler.HasReconciled).To(BeFalse())
			}
		},
		Entry("Finalize", terminationreconcile.PolicyFinalize),
		Entry("Continue", terminationreconcile.PolicyContinue),
	)
})

type TestReconciler struct {
	NodeClaimNotFoundPolicyResult      terminationreconcile.Policy
	TerminationGracePeriodPolicyResult terminationreconcile.Policy
	HasReconciled                      bool
}

func (r *TestReconciler) Reset() {
	*r = TestReconciler{
		NodeClaimNotFoundPolicyResult:      terminationreconcile.PolicyContinue,
		TerminationGracePeriodPolicyResult: terminationreconcile.PolicyContinue,
	}
}

func (r *TestReconciler) Name() string {
	return "test.reconciler"
}

func (r *TestReconciler) AwaitFinalizers() []string {
	return DependentFinalizers.UnsortedList()
}

func (r *TestReconciler) Finalizer() string {
	return TestFinalizer
}

func (r *TestReconciler) NodeClaimNotFoundPolicy() terminationreconcile.Policy {
	return r.NodeClaimNotFoundPolicyResult
}

func (r *TestReconciler) TerminationGracePeriodPolicy() terminationreconcile.Policy {
	return r.TerminationGracePeriodPolicyResult
}

func (r *TestReconciler) Reconcile(_ context.Context, _ *corev1.Node, _ *v1.NodeClaim) (reconcile.Result, error) {
	r.HasReconciled = true
	return reconcile.Result{}, nil
}

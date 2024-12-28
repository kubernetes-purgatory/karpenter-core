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

package instancetermination_test

import (
	"context"
	"testing"
	"time"

	clock "k8s.io/utils/clock/testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/instancetermination"
	terminationreconcile "sigs.k8s.io/karpenter/pkg/controllers/node/termination/reconcile"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var env *test.Environment
var fakeClock *clock.FakeClock
var cloudProvider *fake.CloudProvider
var recorder *test.EventRecorder

var reconciler reconcile.Reconciler

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "InstanceTermination")
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
	reconciler = terminationreconcile.AsReconciler(env.Client, cloudProvider, fakeClock, instancetermination.NewController(fakeClock, env.Client, cloudProvider))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("InstanceTermination", func() {
	var node *corev1.Node
	var nodeClaim *v1.NodeClaim

	BeforeEach(func() {
		fakeClock.SetTime(time.Now())
		cloudProvider.Reset()
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Finalizers: []string{
			v1.TerminationFinalizer,
		}}})
		cloudProvider.CreatedNodeClaims[node.Spec.ProviderID] = nodeClaim
	})
	AfterEach(func() {
		metrics.NodesTerminatedTotal.Reset()
		instancetermination.DurationSeconds.Reset()
		instancetermination.NodeLifetimeDurationSeconds.Reset()
	})

	It("should terminate the instance", func() {
		ExpectApplied(ctx, env.Client, node, nodeClaim)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsUnknown()).To(BeTrue())
		Expect(len(cloudProvider.DeleteCalls)).To(Equal(0))

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue()).To(BeTrue())
		Expect(len(cloudProvider.DeleteCalls)).To(Equal(1))

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		Expect(len(cloudProvider.DeleteCalls)).To(Equal(1))
		ExpectNotFound(ctx, env.Client, node)
	})
	DescribeTable(
		"should ignore nodes with dependent finalizers",
		func(finalizer string) {
			node.Finalizers = append(nodeClaim.Finalizers, finalizer)
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsUnknown()).To(BeTrue())

			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsUnknown()).To(BeTrue())

			node = ExpectExists(ctx, env.Client, node)
			stored := node.DeepCopy()
			node.Finalizers = lo.Reject(node.Finalizers, func(f string, _ int) bool {
				return f == finalizer
			})
			Expect(env.Client.Patch(ctx, node, client.StrategicMergeFrom(stored))).To(Succeed())
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
			Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue()).To(BeTrue())

			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			ExpectNotFound(ctx, env.Client, node)
		},
		Entry("VolumeFinalizer", v1.VolumeFinalizer),
		Entry("DrainFinalizer", v1.DrainFinalizer),
	)
	It("should finalize node when the instance has already been terminated", func() {
		delete(cloudProvider.CreatedNodeClaims, node.Spec.ProviderID)
		ExpectApplied(ctx, env.Client, node, nodeClaim)
		ExpectMakeNodesNotReady(ctx, env.Client, node)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		ExpectExists(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		ExpectNotFound(ctx, env.Client, node)
	})
	Context("Metrics", func() {
		It("should fire the terminationSummary metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))

			m, ok := FindMetricWithLabelValues("karpenter_nodes_termination_duration_seconds", map[string]string{"nodepool": node.Labels[v1.NodePoolLabelKey]})
			Expect(ok).To(BeTrue())
			Expect(m.GetSummary().GetSampleCount()).To(BeNumerically("==", 1))
		})
		It("should fire the nodesTerminated counter metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))

			m, ok := FindMetricWithLabelValues("karpenter_nodes_terminated_total", map[string]string{"nodepool": node.Labels[v1.NodePoolLabelKey]})
			Expect(ok).To(BeTrue())
			Expect(lo.FromPtr(m.GetCounter().Value)).To(BeNumerically("==", 1))
		})
		It("should fire the lifetime duration histogram metric when deleting nodes", func() {
			ExpectApplied(ctx, env.Client, node, nodeClaim)
			Expect(env.Client.Delete(ctx, node)).To(Succeed())
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			// Reconcile twice, once to set the NodeClaim to terminating, another to check the instance termination status (and delete the node).
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))

			m, ok := FindMetricWithLabelValues("karpenter_nodes_lifetime_duration_seconds", map[string]string{"nodepool": node.Labels[v1.NodePoolLabelKey]})
			Expect(ok).To(BeTrue())
			Expect(lo.FromPtr(m.GetHistogram().SampleCount)).To(BeNumerically("==", 1))
		})
	})
})

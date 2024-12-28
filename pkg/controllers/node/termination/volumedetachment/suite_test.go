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

package volumedetachment_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clock "k8s.io/utils/clock/testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	terminationreconcile "sigs.k8s.io/karpenter/pkg/controllers/node/termination/reconcile"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/volumedetachment"
	"sigs.k8s.io/karpenter/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var ctx context.Context
var reconciler reconcile.Reconciler
var env *test.Environment
var fakeClock *clock.FakeClock
var cloudProvider *fake.CloudProvider
var recorder *test.EventRecorder

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "VolumeDetachment")
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
	reconciler = terminationreconcile.AsReconciler(env.Client, cloudProvider, fakeClock, volumedetachment.NewController(fakeClock, env.Client, cloudProvider, recorder))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("VolumeDetachment", func() {
	var node *corev1.Node
	var nodeClaim *v1.NodeClaim

	BeforeEach(func() {
		fakeClock.SetTime(time.Now())
		cloudProvider.Reset()

		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Finalizers: []string{
			v1.DrainFinalizer,
			v1.VolumeFinalizer,
			v1.TerminationFinalizer,
		}}})
		cloudProvider.CreatedNodeClaims[node.Spec.ProviderID] = nodeClaim
	})
	It("should wait for volume attachments", func() {
		va := test.VolumeAttachment(test.VolumeAttachmentOptions{
			NodeName:   node.Name,
			VolumeName: "foo",
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim, va)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsUnknown()).To(BeTrue())

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(v1.VolumeFinalizer))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsFalse()).To(BeTrue())

		ExpectDeleted(ctx, env.Client, va)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.VolumeFinalizer))
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
			// ObjectMeta: metav1.ObjectMeta{
			// 	OwnerReferences: defaultOwnerRefs,
			// },
			Tolerations: []corev1.Toleration{{
				Key:      v1.DisruptedTaintKey,
				Operator: corev1.TolerationOpExists,
			}},
			PersistentVolumeClaims: []string{pvc.Name},
		})
		ExpectApplied(ctx, env.Client, node, nodeClaim, vaDrainable, vaNonDrainable, pod, pvc)
		ExpectManualBinding(ctx, env.Client, pod, node)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsUnknown()).To(BeTrue())

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(v1.VolumeFinalizer))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsFalse()).To(BeTrue())

		ExpectDeleted(ctx, env.Client, vaDrainable)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.VolumeFinalizer))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsTrue()).To(BeTrue())
	})
	It("should remove volume protection finalizer once TGP has elapsed", func() {
		va := test.VolumeAttachment(test.VolumeAttachmentOptions{
			NodeName:   node.Name,
			VolumeName: "foo",
		})
		nodeClaim.Annotations = map[string]string{
			v1.NodeClaimTerminationTimestampAnnotationKey: fakeClock.Now().Add(time.Minute).Format(time.RFC3339),
		}
		ExpectApplied(ctx, env.Client, node, nodeClaim, va)
		Expect(env.Client.Delete(ctx, node)).To(Succeed())
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsUnknown()).To(BeTrue())

		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).To(ContainElement(v1.VolumeFinalizer))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsFalse()).To(BeTrue())

		fakeClock.Step(5 * time.Minute)
		ExpectReconcileSucceeded(ctx, reconciler, client.ObjectKeyFromObject(node))

		// Since TGP expired, the protection finalizer should have been removed. However, the status condition should
		// remain false since volumes were never detached.
		node = ExpectExists(ctx, env.Client, node)
		Expect(node.Finalizers).ToNot(ContainElement(v1.VolumeFinalizer))
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.StatusConditions().Get(v1.ConditionTypeVolumesDetached).IsFalse()).To(BeTrue())
	})
})

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

package podevents_test

import (
	"context"
	"testing"
	"time"

	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	. "sigs.k8s.io/karpenter/pkg/utils/testing"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/podevents"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"

	"sigs.k8s.io/karpenter/pkg/test"
)

var ctx context.Context
var podEventsController *podevents.Controller
var env *test.Environment
var fakeClock *clock.FakeClock
var cp *fake.CloudProvider

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Disruption")
}

var _ = BeforeSuite(func() {
	fakeClock = clock.NewFakeClock(time.Now())
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...), test.WithFieldIndexers(func(c cache.Cache) error {
		return c.IndexField(ctx, &v1.NodeClaim{}, "status.providerID", func(obj client.Object) []string {
			return []string{obj.(*v1.NodeClaim).Status.ProviderID}
		})
	}))
	ctx = options.ToContext(ctx, test.Options())
	cp = fake.NewCloudProvider()
	podEventsController = podevents.NewController(fakeClock, env.Client)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = BeforeEach(func() {
	ctx = options.ToContext(ctx, test.Options())
	fakeClock.SetTime(time.Now())
})

var _ = AfterEach(func() {
	cp.Reset()
	ExpectCleanedUp(ctx, env.Client)
})
var _ = Describe("PodEvents", func() {
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node
	var pod *corev1.Pod

	BeforeEach(func() {
		nodePool = test.NodePool()
		nodeClaim, node = test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: "default-instance-type", // need the instance type for the cluster state update
				},
			},
			Status: v1.NodeClaimStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
		pod = test.Pod(test.PodOptions{
			NodeName: node.Name,
		})
	})
	It("should set the nodeclaim lastPodEvent", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node, pod)
		fakeClock.SetTime(pod.CreationTimestamp.Time)
		ExpectObjectReconciled(ctx, env.Client, podEventsController, pod)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Status.LastPodEventTime.Time).To(BeEquivalentTo(pod.CreationTimestamp.Time))
	})
	It("should not set the nodeclaim lastPodEvent for a node that Karpenter doesn't own", func() {
		delete(node.Labels, v1.NodePoolLabelKey)
		ExpectApplied(ctx, env.Client, nodePool, node, pod)
		fakeClock.SetTime(pod.CreationTimestamp.Time)
		ExpectObjectReconciled(ctx, env.Client, podEventsController, pod)

		Expect(nodeClaim.Status.LastPodEventTime.Time).To(BeZero())
	})
	It("should not set the nodeclaim lastPodEvent when the node does not exist", func() {
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, pod)
		fakeClock.SetTime(pod.CreationTimestamp.Time)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, podEventsController, pod)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Status.LastPodEventTime.Time).To(BeZero())
	})
	It("should not set the nodeclaim lastPodEvent when the nodeclaim does not exist", func() {
		ExpectApplied(ctx, env.Client, nodePool, node, pod)
		fakeClock.SetTime(pod.CreationTimestamp.Time)
		_ = ExpectObjectReconcileFailed(ctx, env.Client, podEventsController, pod)
		Expect(nodeClaim.Status.LastPodEventTime.Time).To(BeZero())
	})
	It("should only set the nodeclaim lastPodEvent when it hasn't been set before", func() {
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, pod)
		fakeClock.SetTime(pod.CreationTimestamp.Time)
		ExpectObjectReconciled(ctx, env.Client, podEventsController, pod)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Status.LastPodEventTime.Time).To(BeEquivalentTo(pod.CreationTimestamp.Time))

		fakeClock.Step(5 * time.Second)
		ExpectObjectReconciled(ctx, env.Client, podEventsController, pod)
		Expect(nodeClaim.Status.LastPodEventTime.Time).To(BeEquivalentTo(pod.CreationTimestamp.Time))
	})
	It("should only set the nodeclaim lastPodEvent once within the dedupe timeframe", func() {
		ExpectApplied(ctx, env.Client, nodePool, node, nodeClaim, pod)
		fakeClock.SetTime(pod.CreationTimestamp.Time)
		ExpectObjectReconciled(ctx, env.Client, podEventsController, pod)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Status.LastPodEventTime.Time).To(BeEquivalentTo(pod.CreationTimestamp.Time))
		lastPodEventTime := nodeClaim.Status.LastPodEventTime.Time

		fakeClock.Step(5 * time.Second)
		ExpectObjectReconciled(ctx, env.Client, podEventsController, pod)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Status.LastPodEventTime.Time).To(BeEquivalentTo(lastPodEventTime))

		fakeClock.Step(5 * time.Second)
		ExpectObjectReconciled(ctx, env.Client, podEventsController, pod)

		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		Expect(nodeClaim.Status.LastPodEventTime.Time).ToNot(BeEquivalentTo(lastPodEventTime))
	})
})

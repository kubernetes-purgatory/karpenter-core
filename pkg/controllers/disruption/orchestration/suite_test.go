/*
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

package orchestration_test

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	. "knative.dev/pkg/logging/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	disruptionevents "github.com/aws/karpenter-core/pkg/controllers/disruption/events"
	"github.com/aws/karpenter-core/pkg/controllers/provisioning"
	"github.com/aws/karpenter-core/pkg/utils/nodeclaim"

	"github.com/aws/karpenter-core/pkg/apis"
	"github.com/aws/karpenter-core/pkg/apis/settings"
	"github.com/aws/karpenter-core/pkg/apis/v1beta1"
	"github.com/aws/karpenter-core/pkg/cloudprovider/fake"
	"github.com/aws/karpenter-core/pkg/controllers/disruption/orchestration"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/controllers/state/informer"
	"github.com/aws/karpenter-core/pkg/operator/controller"
	"github.com/aws/karpenter-core/pkg/operator/scheme"
	"github.com/aws/karpenter-core/pkg/test"
	. "github.com/aws/karpenter-core/pkg/test/expectations"

	v1 "k8s.io/api/core/v1"
	clock "k8s.io/utils/clock/testing"
)

var ctx context.Context
var env *test.Environment
var cluster *state.Cluster
var cloudProvider *fake.CloudProvider
var nodeStateController controller.Controller
var nodeClaimStateController controller.Controller
var fakeClock *clock.FakeClock
var recorder *test.EventRecorder
var queue *orchestration.Queue
var prov *provisioning.Provisioner

var replacements []nodeclaim.Key
var ncKey nodeclaim.Key

var nodeClaim1, nodeClaim2, replacementNodeClaim *v1beta1.NodeClaim
var nodePool *v1beta1.NodePool
var node1, node2, replacementNode *v1.Node

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Disruption/Orchestration")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(scheme.Scheme, test.WithCRDs(apis.CRDs...))
	ctx = settings.ToContext(ctx, test.Settings(settings.Settings{DriftEnabled: true}))
	fakeClock = clock.NewFakeClock(time.Now())
	cloudProvider = fake.NewCloudProvider()
	cluster = state.NewCluster(fakeClock, env.Client, cloudProvider)
	nodeStateController = informer.NewNodeController(env.Client, cluster)
	nodeClaimStateController = informer.NewNodeClaimController(env.Client, cluster)
	recorder = test.NewEventRecorder()
	prov = provisioning.NewProvisioner(env.Client, env.KubernetesInterface.CoreV1(), recorder, cloudProvider, cluster)
	queue = orchestration.NewTestingQueue(env.Client, recorder, cluster, fakeClock, prov)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = BeforeEach(func() {
	recorder.Reset() // Reset the events that we captured during the run

	fakeClock.SetTime(time.Now())
	cluster.Reset()
	cloudProvider.Reset()
	cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
	cluster.MarkUnconsolidated()
	queue.Reset()
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
})

var _ = Describe("Queue", func() {
	BeforeEach(func() {
		nodePool = test.NodePool()
		nodeClaim1, node1 = test.NodeClaimAndNode(
			v1beta1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1beta1.NodePoolLabelKey:     nodePool.Name,
						v1.LabelInstanceTypeStable:   cloudProvider.InstanceTypes[0].Name,
						v1beta1.CapacityTypeLabelKey: cloudProvider.InstanceTypes[0].Offerings.Cheapest().CapacityType,
						v1.LabelTopologyZone:         cloudProvider.InstanceTypes[0].Offerings.Cheapest().Zone,
					},
				},
				Status: v1beta1.NodeClaimStatus{
					ProviderID:  test.RandomProviderID(),
					Allocatable: map[v1.ResourceName]resource.Quantity{v1.ResourceCPU: resource.MustParse("32")},
				},
			},
		)
		nodeClaim2, node2 = test.NodeClaimAndNode(
			v1beta1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1beta1.NodePoolLabelKey:     nodePool.Name,
						v1.LabelInstanceTypeStable:   cloudProvider.InstanceTypes[0].Name,
						v1beta1.CapacityTypeLabelKey: cloudProvider.InstanceTypes[0].Offerings.Cheapest().CapacityType,
						v1.LabelTopologyZone:         cloudProvider.InstanceTypes[0].Offerings.Cheapest().Zone,
					},
				},
				Status: v1beta1.NodeClaimStatus{
					ProviderID:  test.RandomProviderID(),
					Allocatable: map[v1.ResourceName]resource.Quantity{v1.ResourceCPU: resource.MustParse("32")},
				},
			},
		)
		node1.Spec.Taints = append(node1.Spec.Taints, v1beta1.DisruptionNoScheduleTaint)
		node2.Spec.Taints = append(node2.Spec.Taints, v1beta1.DisruptionNoScheduleTaint)

		ncKey = nodeclaim.Key{
			Name:      test.RandomName(),
			IsMachine: true,
		}
		replacements = []nodeclaim.Key{ncKey}
		replacementNodeClaim, replacementNode = test.NodeClaimAndNode(
			v1beta1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: ncKey.Name,
					Labels: map[string]string{
						v1beta1.NodePoolLabelKey:     nodePool.Name,
						v1.LabelInstanceTypeStable:   cloudProvider.InstanceTypes[0].Name,
						v1beta1.CapacityTypeLabelKey: cloudProvider.InstanceTypes[0].Offerings.Cheapest().CapacityType,
						v1.LabelTopologyZone:         cloudProvider.InstanceTypes[0].Offerings.Cheapest().Zone,
					},
				},
				Status: v1beta1.NodeClaimStatus{
					ProviderID:  test.RandomProviderID(),
					Allocatable: map[v1.ResourceName]resource.Quantity{v1.ResourceCPU: resource.MustParse("32")},
				},
			},
		)
	})

	Context("Add", func() {
		It("should remove the karpenter.sh/disruption taint for nodes that fail to disrupt", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*v1.Node{node1}, []*v1beta1.NodeClaim{nodeClaim1})

			stateNode := ExpectStateNodeExists(cluster, node1)
			Expect(queue.Add(orchestration.NewCommand(replacements, []*state.StateNode{stateNode}, fakeClock.Now().Add(-100*time.Minute), "test-method", "fake-type"))).To(BeNil())

			ExpectReconcileSucceeded(ctx, queue, types.NamespacedName{})
			node1 = ExpectNodeExists(ctx, env.Client, node1.Name)
			Expect(node1.Spec.Taints).ToNot(ContainElement(v1beta1.DisruptionNoScheduleTaint))
		})
		It("should keep nodes tainted when replacements launch successfully", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool, replacementNodeClaim, replacementNode)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*v1.Node{node1}, []*v1beta1.NodeClaim{nodeClaim1})

			stateNode := ExpectStateNodeExists(cluster, node1)
			Expect(queue.Add(orchestration.NewCommand(replacements, []*state.StateNode{stateNode}, fakeClock.Now(), "test-method", "fake-type"))).To(BeNil())

			node1 = ExpectNodeExists(ctx, env.Client, node1.Name)
			Expect(node1.Spec.Taints).To(ContainElement(v1beta1.DisruptionNoScheduleTaint))

			// Update state
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))
			Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))
			Expect(node1.Spec.Taints).To(ContainElement(v1beta1.DisruptionNoScheduleTaint))
		})
	})
	Context("Reconcile", func() {
		It("should not return an error when handling commands before the timeout", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool, replacementNodeClaim)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*v1.Node{node1}, []*v1beta1.NodeClaim{nodeClaim1})
			stateNode := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim1)

			Expect(queue.Add(orchestration.NewCommand(replacements, []*state.StateNode{stateNode}, fakeClock.Now(), "test-method", "fake-type"))).To(BeNil())
			ExpectReconcileSucceeded(ctx, queue, types.NamespacedName{})
		})
		It("should return an error and clean up when a command times out", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*v1.Node{node1}, []*v1beta1.NodeClaim{nodeClaim1})
			stateNode := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim1)

			Expect(queue.Add(orchestration.NewCommand(replacements, []*state.StateNode{stateNode}, fakeClock.Now(), "test-method", "fake-type"))).To(BeNil())

			fakeClock.Step(1 * time.Hour)
			ExpectReconcileSucceeded(ctx, queue, types.NamespacedName{})
			node1 = ExpectNodeExists(ctx, env.Client, node1.Name)
			Expect(node1.Spec.Taints).ToNot(ContainElement(v1beta1.DisruptionNoScheduleTaint))
			_, ok := queue.ProviderIDToCommand[stateNode.ProviderID()]
			Expect(ok).To(BeFalse())
		})
		It("should fully handle a command when replacements are initialized", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool, replacementNodeClaim, replacementNode)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*v1.Node{node1}, []*v1beta1.NodeClaim{nodeClaim1})
			stateNode := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim1)

			Expect(queue.Add(orchestration.NewCommand(replacements, []*state.StateNode{stateNode}, fakeClock.Now(), "test-method", "fake-type"))).To(BeNil())
			ExpectReconcileSucceeded(ctx, queue, types.NamespacedName{})

			// Get the command
			cmd := queue.ProviderIDToCommand[nodeClaim1.Status.ProviderID]
			Expect(cmd).ToNot(BeNil())
			Expect(cmd.ReplacementKeys[0].Initialized).To(BeFalse())

			Expect(recorder.DetectedEvent(disruptionevents.Launching(replacementNodeClaim, cmd.Reason()).Message)).To(BeTrue())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(replacementNodeClaim).Message)).To(BeTrue())

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController,
				[]*v1.Node{replacementNode}, []*v1beta1.NodeClaim{replacementNodeClaim})

			ExpectReconcileSucceeded(ctx, queue, types.NamespacedName{})
			Expect(cmd.ReplacementKeys[0].Initialized).To(BeTrue())

			terminatingEvents := disruptionevents.Terminating(node1, nodeClaim1, cmd.Reason())
			Expect(recorder.DetectedEvent(terminatingEvents[0].Message)).To(BeTrue())
			Expect(recorder.DetectedEvent(terminatingEvents[1].Message)).To(BeTrue())

			ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
			// And expect the nodeClaim and node to be deleted
			ExpectNotFound(ctx, env.Client, nodeClaim1, node1)
		})
		It("should only finish a command when all replacements are initialized", func() {
			ncKey2 := nodeclaim.Key{
				Name:      test.RandomName(),
				IsMachine: false,
			}
			replacements = []nodeclaim.Key{ncKey, ncKey2}
			replacementnodeClaim2, replacementNode2 := test.NodeClaimAndNode(v1beta1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: ncKey2.Name,
				},
			})

			ExpectApplied(ctx, env.Client, nodeClaim1, node1, replacementNodeClaim, replacementNode, replacementnodeClaim2, replacementNode2, nodePool)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*v1.Node{node1}, []*v1beta1.NodeClaim{nodeClaim1})
			stateNode := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim1)

			cmd := orchestration.NewCommand(replacements, []*state.StateNode{stateNode}, fakeClock.Now(), "test-method", "fake-type")
			Expect(queue.Add(cmd)).To(BeNil())

			ExpectReconcileSucceeded(ctx, queue, types.NamespacedName{})
			Expect(cmd.ReplacementKeys[0].Initialized).To(BeFalse())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(nodeClaim1).Message)).To(BeTrue())
			Expect(cmd.ReplacementKeys[1].Initialized).To(BeFalse())

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*v1.Node{replacementNode}, []*v1beta1.NodeClaim{replacementNodeClaim})

			ExpectReconcileSucceeded(ctx, queue, types.NamespacedName{})
			Expect(cmd.ReplacementKeys[0].Initialized).To(BeTrue())
			Expect(cmd.ReplacementKeys[1].Initialized).To(BeFalse())
			Expect(recorder.DetectedEvent(disruptionevents.WaitingOnReadiness(nodeClaim1).Message)).To(BeTrue())

			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*v1.Node{replacementNode2}, []*v1beta1.NodeClaim{replacementnodeClaim2})

			ExpectReconcileSucceeded(ctx, queue, types.NamespacedName{})
			Expect(cmd.ReplacementKeys[0].Initialized).To(BeTrue())
			Expect(cmd.ReplacementKeys[1].Initialized).To(BeTrue())

			ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
			// And expect the nodeClaim and node to be deleted
			ExpectNotFound(ctx, env.Client, nodeClaim1, node1)
		})
		It("should not wait for replacements when none are needed", func() {
			ExpectApplied(ctx, env.Client, nodeClaim1, node1, nodePool)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*v1.Node{node1}, []*v1beta1.NodeClaim{nodeClaim1})
			stateNode := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim1)
			Expect(queue.Add(orchestration.NewCommand([]nodeclaim.Key{}, []*state.StateNode{stateNode}, fakeClock.Now(), "test-method", "fake-type"))).To(BeNil())

			// Get the command and process it
			cmd := queue.ProviderIDToCommand[nodeClaim1.Status.ProviderID]
			Expect(cmd).ToNot(BeNil())
			ExpectReconcileSucceeded(ctx, queue, types.NamespacedName{})

			terminatingEvents := disruptionevents.Terminating(node1, nodeClaim1, cmd.Reason())
			Expect(recorder.DetectedEvent(terminatingEvents[0].Message)).To(BeTrue())
			Expect(recorder.DetectedEvent(terminatingEvents[1].Message)).To(BeTrue())

			ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim1)
			// And expect the nodeClaim and node to be deleted
			ExpectNotFound(ctx, env.Client, nodeClaim1, node1)
		})
	})
})

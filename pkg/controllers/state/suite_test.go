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

package state_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	clock "k8s.io/utils/clock/testing"
	"knative.dev/pkg/ptr"

	"github.com/aws/karpenter-core/pkg/apis"
	"github.com/aws/karpenter-core/pkg/apis/settings"
	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/controllers/state/informer"
	"github.com/aws/karpenter-core/pkg/operator/controller"
	"github.com/aws/karpenter-core/pkg/operator/scheme"

	"github.com/aws/karpenter-core/pkg/cloudprovider/fake"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "knative.dev/pkg/logging/testing"

	. "github.com/aws/karpenter-core/pkg/test/expectations"
)

var ctx context.Context
var env *test.Environment
var fakeClock *clock.FakeClock
var cluster *state.Cluster
var machineController controller.Controller
var nodeController controller.Controller
var podController controller.Controller
var provisionerController controller.Controller
var daemonsetController controller.Controller
var cloudProvider *fake.CloudProvider
var provisioner *v1alpha5.Provisioner

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controllers/State")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(scheme.Scheme, test.WithCRDs(apis.CRDs...))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = BeforeEach(func() {
	ctx = settings.ToContext(ctx, test.Settings())
	cloudProvider = fake.NewCloudProvider()
	cloudProvider.InstanceTypes = fake.InstanceTypesAssorted()
	fakeClock = clock.NewFakeClock(time.Now())
	cluster = state.NewCluster(fakeClock, env.Client, cloudProvider)
	machineController = informer.NewMachineController(env.Client, cluster)
	nodeController = informer.NewNodeController(env.Client, cluster)
	podController = informer.NewPodController(env.Client, cluster)
	provisionerController = informer.NewProvisionerController(env.Client, cluster)
	daemonsetController = informer.NewDaemonSetController(env.Client, cluster)
	provisioner = test.Provisioner(test.ProvisionerOptions{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	ExpectApplied(ctx, env.Client, provisioner)
})
var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
})

var _ = Describe("Inflight Nodes", func() {
	It("should consider the node capacity/allocatable based on the instance type", func() {
		instanceType := cloudProvider.InstanceTypes[0]
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       instanceType.Name,
			}},
		})
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		ExpectStateNodeCount("==", 1)
		ExpectResources(instanceType.Allocatable(), ExpectStateNodeExists(node).Allocatable())
		ExpectResources(instanceType.Capacity, ExpectStateNodeExists(node).Capacity())
	})
	It("should consider the node capacity/allocatable as a combination of instance type and current node", func() {
		instanceType := cloudProvider.InstanceTypes[0]
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       instanceType.Name,
			}},
			Allocatable: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("100Mi"),
			},
			Capacity: v1.ResourceList{
				v1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
			},
		})
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		ExpectStateNodeCount("==", 1)
		ExpectResources(v1.ResourceList{
			v1.ResourceMemory:           resource.MustParse("100Mi"), // pulled from the node's real allocatable
			v1.ResourceCPU:              *instanceType.Capacity.Cpu(),
			v1.ResourceEphemeralStorage: *instanceType.Capacity.StorageEphemeral(),
		}, ExpectStateNodeExists(node).Allocatable())
		ExpectResources(v1.ResourceList{
			v1.ResourceMemory:           *instanceType.Capacity.Memory(),
			v1.ResourceCPU:              *instanceType.Capacity.Cpu(),
			v1.ResourceEphemeralStorage: resource.MustParse("100Gi"), // pulled from the node's real capacity
		}, ExpectStateNodeExists(node).Capacity())
	})
	It("should ignore machines that don't yet have provider id", func() {
		machine := test.Machine(v1alpha5.Machine{
			Spec: v1alpha5.MachineSpec{
				Taints: []v1.Taint{
					{
						Key:    "custom-taint",
						Value:  "custom-value",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
				Requirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelInstanceTypeStable,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{cloudProvider.InstanceTypes[0].Name},
					},
					{
						Key:      v1.LabelTopologyZone,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"test-zone-1"},
					},
				},
				Resources: v1alpha5.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:              resource.MustParse("2"),
						v1.ResourceMemory:           resource.MustParse("2Gi"),
						v1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
					},
				},
				MachineTemplateRef: &v1alpha5.ProviderRef{
					Name: "default",
				},
			},
		})
		machine.Status.ProviderID = ""
		ExpectApplied(ctx, env.Client, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		ExpectStateNodeNotFoundForMachine(machine)
	})
	It("should model the inflight data as machine with no node", func() {
		machine := test.Machine(v1alpha5.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					v1alpha5.DoNotConsolidateNodeAnnotationKey: "true",
				},
				Labels: map[string]string{
					v1alpha5.ProvisionerNameLabelKey: "default",
					v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
					v1.LabelTopologyZone:             "test-zone-1",
					v1.LabelTopologyRegion:           "test-region",
					v1.LabelHostname:                 "custom-host-name",
				},
			},
			Spec: v1alpha5.MachineSpec{
				Taints: []v1.Taint{
					{
						Key:    "custom-taint",
						Value:  "custom-value",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
				Requirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelInstanceTypeStable,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{cloudProvider.InstanceTypes[0].Name},
					},
					{
						Key:      v1.LabelTopologyZone,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"test-zone-1"},
					},
				},
				Resources: v1alpha5.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:              resource.MustParse("2"),
						v1.ResourceMemory:           resource.MustParse("2Gi"),
						v1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
					},
				},
				MachineTemplateRef: &v1alpha5.ProviderRef{
					Name: "default",
				},
			},
			Status: v1alpha5.MachineStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
		ExpectApplied(ctx, env.Client, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))

		ExpectStateNodeCount("==", 1)
		stateNode := ExpectStateNodeExistsForMachine(machine)
		Expect(stateNode.Labels()).To(HaveKeyWithValue(v1alpha5.ProvisionerNameLabelKey, "default"))
		Expect(stateNode.Labels()).To(HaveKeyWithValue(v1.LabelInstanceTypeStable, cloudProvider.InstanceTypes[0].Name))
		Expect(stateNode.Labels()).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-1"))
		Expect(stateNode.Labels()).To(HaveKeyWithValue(v1.LabelTopologyRegion, "test-region"))
		Expect(stateNode.Labels()).To(HaveKeyWithValue(v1.LabelHostname, "custom-host-name"))
		Expect(stateNode.HostName()).To(Equal("custom-host-name"))
		Expect(stateNode.Annotations()).To(HaveKeyWithValue(v1alpha5.DoNotConsolidateNodeAnnotationKey, "true"))
		Expect(stateNode.Initialized()).To(BeFalse())
		Expect(stateNode.Owned()).To(BeTrue())
	})
	It("should model the inflight capacity/allocatable as the machine capacity/allocatable", func() {
		machine := test.Machine(v1alpha5.Machine{
			Spec: v1alpha5.MachineSpec{
				Requirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelInstanceTypeStable,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{cloudProvider.InstanceTypes[0].Name},
					},
					{
						Key:      v1.LabelTopologyZone,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"test-zone-1"},
					},
				},
				MachineTemplateRef: &v1alpha5.ProviderRef{
					Name: "default",
				},
			},
			Status: v1alpha5.MachineStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("2"),
					v1.ResourceMemory:           resource.MustParse("32Gi"),
					v1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
				},
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("1"),
					v1.ResourceMemory:           resource.MustParse("30Gi"),
					v1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
				},
			},
		})
		ExpectApplied(ctx, env.Client, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))

		ExpectStateNodeCount("==", 1)
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("2"),
			v1.ResourceMemory:           resource.MustParse("32Gi"),
			v1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
		}, ExpectStateNodeExistsForMachine(machine).Capacity())
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("1"),
			v1.ResourceMemory:           resource.MustParse("30Gi"),
			v1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
		}, ExpectStateNodeExistsForMachine(machine).Allocatable())
	})
	It("should model the inflight capacity of the machine until the node registers and is initialized", func() {
		machine := test.Machine(v1alpha5.Machine{
			Spec: v1alpha5.MachineSpec{
				Requirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelInstanceTypeStable,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{cloudProvider.InstanceTypes[0].Name},
					},
					{
						Key:      v1.LabelTopologyZone,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"test-zone-1"},
					},
				},
				MachineTemplateRef: &v1alpha5.ProviderRef{
					Name: "default",
				},
			},
			Status: v1alpha5.MachineStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("2"),
					v1.ResourceMemory:           resource.MustParse("32Gi"),
					v1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
				},
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("1"),
					v1.ResourceMemory:           resource.MustParse("30Gi"),
					v1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
				},
			},
		})
		ExpectApplied(ctx, env.Client, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))

		ExpectStateNodeCount("==", 1)
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("2"),
			v1.ResourceMemory:           resource.MustParse("32Gi"),
			v1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
		}, ExpectStateNodeExistsForMachine(machine).Capacity())
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("1"),
			v1.ResourceMemory:           resource.MustParse("30Gi"),
			v1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
		}, ExpectStateNodeExistsForMachine(machine).Allocatable())

		node := test.Node(test.NodeOptions{
			ProviderID: machine.Status.ProviderID,
			Capacity: v1.ResourceList{
				v1.ResourceCPU:              resource.MustParse("1800m"),
				v1.ResourceMemory:           resource.MustParse("30500Mi"),
				v1.ResourceEphemeralStorage: resource.MustParse("19000Mi"),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:              resource.MustParse("900m"),
				v1.ResourceMemory:           resource.MustParse("29250Mi"),
				v1.ResourceEphemeralStorage: resource.MustParse("17800Mi"),
			},
		})
		ExpectApplied(ctx, env.Client, node)

		ExpectStateNodeCount("==", 1)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		// Update machine to be initialized
		machine.StatusConditions().MarkTrue(v1alpha5.MachineInitialized)
		ExpectApplied(ctx, env.Client, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))

		ExpectStateNodeCount("==", 1)
		ExpectStateNodeExistsForMachine(machine)
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("1800m"),
			v1.ResourceMemory:           resource.MustParse("30500Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("19000Mi"),
		}, ExpectStateNodeExists(node).Capacity())
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("900m"),
			v1.ResourceMemory:           resource.MustParse("29250Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("17800Mi"),
		}, ExpectStateNodeExists(node).Allocatable())
	})
	It("should combine the inflight capacity with node while node isn't initialized", func() {
		machine := test.Machine(v1alpha5.Machine{
			Spec: v1alpha5.MachineSpec{
				Requirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelInstanceTypeStable,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{cloudProvider.InstanceTypes[0].Name},
					},
					{
						Key:      v1.LabelTopologyZone,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"test-zone-1"},
					},
				},
				MachineTemplateRef: &v1alpha5.ProviderRef{
					Name: "default",
				},
			},
			Status: v1alpha5.MachineStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("2"),
					v1.ResourceMemory:           resource.MustParse("32Gi"),
					v1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
				},
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("1"),
					v1.ResourceMemory:           resource.MustParse("30Gi"),
					v1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
				},
			},
		})
		ExpectApplied(ctx, env.Client, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		ExpectStateNodeCount("==", 1)

		node := test.Node(test.NodeOptions{
			ProviderID: machine.Status.ProviderID,
			Capacity: v1.ResourceList{
				v1.ResourceCPU:              resource.MustParse("1800m"),
				v1.ResourceMemory:           resource.MustParse("0"), // Should use the inflight capacity for this value
				v1.ResourceEphemeralStorage: resource.MustParse("19000Mi"),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:              resource.MustParse("0"), // Should use the inflight allocatable for this value
				v1.ResourceMemory:           resource.MustParse("29250Mi"),
				v1.ResourceEphemeralStorage: resource.MustParse("0"), // Should use the inflight allocatable for this value
			},
		})
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)

		// Machine isn't initialized yet so the resources should remain as the in-flight resources
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("1800m"),
			v1.ResourceMemory:           resource.MustParse("32Gi"),
			v1.ResourceEphemeralStorage: resource.MustParse("19000Mi"),
		}, ExpectStateNodeExistsForMachine(machine).Capacity())
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("1"),
			v1.ResourceMemory:           resource.MustParse("29250Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
		}, ExpectStateNodeExistsForMachine(machine).Allocatable())
	})
	It("should continue node nomination when an inflight node becomes a real node", func() {
		machine := test.Machine(v1alpha5.Machine{
			Status: v1alpha5.MachineStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
		ExpectApplied(ctx, env.Client, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		ExpectStateNodeCount("==", 1)
		cluster.NominateNodeForPod(ctx, machine.Name)
		Expect(ExpectStateNodeExistsForMachine(machine).Nominated()).To(BeTrue())

		node := test.Node(test.NodeOptions{
			ProviderID: machine.Status.ProviderID,
		})
		node.Spec.ProviderID = machine.Status.ProviderID
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)
		Expect(ExpectStateNodeExists(node).Nominated()).To(BeTrue())
	})
	It("should continue MarkedForDeletion when an inflight node becomes a real node", func() {
		machine := test.Machine(v1alpha5.Machine{
			Status: v1alpha5.MachineStatus{
				ProviderID: test.RandomProviderID(),
			},
		})
		ExpectApplied(ctx, env.Client, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		ExpectStateNodeCount("==", 1)
		cluster.MarkForDeletion(machine.Name)
		Expect(ExpectStateNodeExistsForMachine(machine).MarkedForDeletion()).To(BeTrue())

		node := test.Node(test.NodeOptions{
			ProviderID: machine.Status.ProviderID,
		})
		node.Spec.ProviderID = machine.Status.ProviderID
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)
		Expect(ExpectStateNodeExists(node).MarkedForDeletion()).To(BeTrue())
	})
})

var _ = Describe("Node Resource Level", func() {
	It("should not count pods not bound to nodes", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("2"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		// two pods, but neither is bound to the node so the node's CPU requests should be zero
		ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("0.0")}, ExpectStateNodeExists(node).PodRequests())
	})
	It("should count new pods bound to nodes", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("2"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("1.5")}, ExpectStateNodeExists(node).PodRequests())

		ExpectManualBinding(ctx, env.Client, pod2, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))
		ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("3.5")}, ExpectStateNodeExists(node).PodRequests())
	})
	It("should count existing pods bound to nodes", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("2"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})

		// simulate a node that already exists in our cluster
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectManualBinding(ctx, env.Client, pod2, node)

		// that we just noticed
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("3.5")}, ExpectStateNodeExists(node).PodRequests())
	})
	It("should subtract requests if the pod is deleted", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("2"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectManualBinding(ctx, env.Client, pod2, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("3.5")}, ExpectStateNodeExists(node).PodRequests())

		// delete the pods and the CPU usage should go down
		ExpectDeleted(ctx, env.Client, pod2)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))
		ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("1.5")}, ExpectStateNodeExists(node).PodRequests())

		ExpectDeleted(ctx, env.Client, pod1)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("0")}, ExpectStateNodeExists(node).PodRequests())
	})
	It("should not add requests if the pod is terminal", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
			Phase: v1.PodFailed,
		})
		pod2 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("2"),
				}},
			Phase: v1.PodSucceeded,
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			},
		})
		ExpectApplied(ctx, env.Client, pod1, pod2)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectManualBinding(ctx, env.Client, pod2, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("0")}, ExpectStateNodeExists(node).PodRequests())
	})
	It("should stop tracking nodes that are deleted", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})
		ExpectApplied(ctx, env.Client, pod1)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		cluster.ForEachNode(func(n *state.Node) bool {
			ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("2.5")}, n.Available())
			ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("1.5")}, n.PodRequests())
			return true
		})

		// delete the node and the internal state should disappear as well
		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		cluster.ForEachNode(func(n *state.Node) bool {
			Fail("shouldn't be called as the node was deleted")
			return true
		})
	})
	It("should track pods correctly if we miss events or they are consolidated", func() {
		pod1 := test.UnschedulablePod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Name: "stateful-set-pod"},
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})

		node1 := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})
		ExpectApplied(ctx, env.Client, pod1, node1)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node1))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		ExpectManualBinding(ctx, env.Client, pod1, node1)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		cluster.ForEachNode(func(n *state.Node) bool {
			ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("2.5")}, n.Available())
			ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("1.5")}, n.PodRequests())
			return true
		})

		ExpectDeleted(ctx, env.Client, pod1)

		// second node has more capacity
		node2 := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("8"),
			}})

		// and the pod can only bind to node2 due to the resource request
		pod2 := test.UnschedulablePod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Name: "stateful-set-pod"},
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("5.0"),
				}},
		})

		ExpectApplied(ctx, env.Client, pod2, node2)
		ExpectManualBinding(ctx, env.Client, pod2, node2)
		// deleted the pod and then recreated it, but simulated only receiving an event on the new pod after it has
		// bound and not getting the new node event entirely
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node2))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod2))

		cluster.ForEachNode(func(n *state.Node) bool {
			if n.Node.Name == node1.Name {
				// not on node1 any longer, so it should be fully free
				ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("4")}, n.Available())
				ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("0")}, n.PodRequests())
			} else {
				ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")}, n.Available())
				ExpectResources(v1.ResourceList{v1.ResourceCPU: resource.MustParse("5")}, n.PodRequests())
			}
			return true
		})

	})
	// nolint:gosec
	It("should maintain a correct count of resource usage as pods are deleted/added", func() {
		var pods []*v1.Pod
		for i := 0; i < 100; i++ {
			pods = append(pods, test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						v1.ResourceCPU: resource.MustParse(fmt.Sprintf("%1.1f", rand.Float64()*2)),
					}},
			}))
		}
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU:  resource.MustParse("200"),
				v1.ResourcePods: resource.MustParse("500"),
			}})
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:  resource.MustParse("0"),
			v1.ResourcePods: resource.MustParse("0"),
		}, ExpectStateNodeExists(node).PodRequests())
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		sum := 0.0
		podCount := 0
		for _, pod := range pods {
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
			podCount++

			// extra reconciles shouldn't cause it to be multiply counted
			nReconciles := rand.Intn(3) + 1 // 1 to 3 reconciles
			for i := 0; i < nReconciles; i++ {
				ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
			}
			sum += pod.Spec.Containers[0].Resources.Requests.Cpu().AsApproximateFloat64()
			ExpectResources(v1.ResourceList{
				v1.ResourceCPU:  resource.MustParse(fmt.Sprintf("%1.1f", sum)),
				v1.ResourcePods: resource.MustParse(fmt.Sprintf("%d", podCount)),
			}, ExpectStateNodeExists(node).PodRequests())
		}

		for _, pod := range pods {
			ExpectDeleted(ctx, env.Client, pod)
			nReconciles := rand.Intn(3) + 1
			// or multiply removed
			for i := 0; i < nReconciles; i++ {
				ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
			}
			sum -= pod.Spec.Containers[0].Resources.Requests.Cpu().AsApproximateFloat64()
			podCount--
			ExpectResources(v1.ResourceList{
				v1.ResourceCPU:  resource.MustParse(fmt.Sprintf("%1.1f", sum)),
				v1.ResourcePods: resource.MustParse(fmt.Sprintf("%d", podCount)),
			}, ExpectStateNodeExists(node).PodRequests())
		}
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:  resource.MustParse("0"),
			v1.ResourcePods: resource.MustParse("0"),
		}, ExpectStateNodeExists(node).PodRequests())
	})
	It("should track daemonset requested resources separately", func() {
		ds := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("2Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, ds)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(ds), ds)).To(Succeed())

		pod1 := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
		})

		dsPod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				}},
		})
		dsPod.OwnerReferences = append(dsPod.OwnerReferences, metav1.OwnerReference{
			APIVersion:         "apps/v1",
			Kind:               "DaemonSet",
			Name:               ds.Name,
			UID:                ds.UID,
			Controller:         ptr.Bool(true),
			BlockOwnerDeletion: ptr.Bool(true),
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU:    resource.MustParse("4"),
				v1.ResourceMemory: resource.MustParse("8Gi"),
			}})
		ExpectApplied(ctx, env.Client, pod1, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		ExpectManualBinding(ctx, env.Client, pod1, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod1))

		// daemonset pod isn't bound yet
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("0"),
			v1.ResourceMemory: resource.MustParse("0"),
		}, ExpectStateNodeExists(node).DaemonSetRequests())
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU: resource.MustParse("1.5"),
		}, ExpectStateNodeExists(node).PodRequests())

		ExpectApplied(ctx, env.Client, dsPod)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(dsPod))
		ExpectManualBinding(ctx, env.Client, dsPod, node)
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(dsPod))

		// just the DS request portion
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("2Gi"),
		}, ExpectStateNodeExists(node).DaemonSetRequests())
		// total request
		ExpectResources(v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("2.5"),
			v1.ResourceMemory: resource.MustParse("2Gi"),
		}, ExpectStateNodeExists(node).PodRequests())
	})
	It("should mark node for deletion when node is deleted", func() {
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
					v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
				},
				Finalizers: []string{v1alpha5.TerminationFinalizer},
			},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}},
		)
		ExpectApplied(ctx, env.Client, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)

		Expect(env.Client.Delete(ctx, node)).To(Succeed())

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectNodeExists(ctx, env.Client, node.Name)
		Expect(ExpectStateNodeExists(node).MarkedForDeletion()).To(BeTrue())
	})
	It("should mark node for deletion when machine is deleted", func() {
		machine := test.Machine(v1alpha5.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Finalizers: []string{v1alpha5.TerminationFinalizer},
			},
			Spec: v1alpha5.MachineSpec{
				Requirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelInstanceTypeStable,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{cloudProvider.InstanceTypes[0].Name},
					},
					{
						Key:      v1.LabelTopologyZone,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"test-zone-1"},
					},
				},
				MachineTemplateRef: &v1alpha5.ProviderRef{
					Name: "default",
				},
			},
			Status: v1alpha5.MachineStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("2"),
					v1.ResourceMemory:           resource.MustParse("32Gi"),
					v1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
				},
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("1"),
					v1.ResourceMemory:           resource.MustParse("30Gi"),
					v1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
				},
			},
		})
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
					v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
				},
				Finalizers: []string{v1alpha5.TerminationFinalizer},
			},
			ProviderID: machine.Status.ProviderID,
		})
		ExpectApplied(ctx, env.Client, machine, node)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectStateNodeCount("==", 1)

		Expect(env.Client.Delete(ctx, machine)).To(Succeed())
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		ExpectExists(ctx, env.Client, machine)

		Expect(ExpectStateNodeExistsForMachine(machine).MarkedForDeletion()).To(BeTrue())
		Expect(ExpectStateNodeExists(node).MarkedForDeletion()).To(BeTrue())
	})
	It("should nominate the node until the nomination time passes", func() {
		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
					v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
				},
				Finalizers: []string{v1alpha5.TerminationFinalizer},
			},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}},
		)
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		cluster.NominateNodeForPod(ctx, node.Name)

		// Expect that the node is now nominated
		Expect(ExpectStateNodeExists(node).Nominated()).To(BeTrue())
		time.Sleep(time.Second * 5) // nomination window is 10s so it should still be nominated
		Expect(ExpectStateNodeExists(node).Nominated()).To(BeTrue())
		time.Sleep(time.Second * 6) // past 10s, node should no longer be nominated
		Expect(ExpectStateNodeExists(node).Nominated()).To(BeFalse())
	})
	It("should handle a node changing from no providerID to registering a providerID", func() {
		node := test.Node()
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		ExpectStateNodeCount("==", 1)
		ExpectStateNodeExists(node)

		// Change the providerID; this mocks CCM adding the providerID onto the node after registration
		node.Spec.ProviderID = fmt.Sprintf("fake://%s", node.Name)
		ExpectApplied(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		ExpectStateNodeCount("==", 1)
		ExpectStateNodeExists(node)
	})
})

var _ = Describe("Pod Anti-Affinity", func() {
	It("should track pods with required anti-affinity", func() {
		pod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
			PodAntiRequirements: []v1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"foo": "bar"},
					},
					TopologyKey: v1.LabelTopologyZone,
				},
			},
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})

		ExpectApplied(ctx, env.Client, pod)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
		foundPodCount := 0
		cluster.ForPodsWithAntiAffinity(func(p *v1.Pod, n *v1.Node) bool {
			foundPodCount++
			Expect(p.Name).To(Equal(pod.Name))
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 1))
	})
	It("should not track pods with preferred anti-affinity", func() {
		pod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
			PodAntiPreferences: []v1.WeightedPodAffinityTerm{
				{
					Weight: 15,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"foo": "bar"},
						},
						TopologyKey: v1.LabelTopologyZone,
					},
				},
			},
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})

		ExpectApplied(ctx, env.Client, pod)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
		foundPodCount := 0
		cluster.ForPodsWithAntiAffinity(func(p *v1.Pod, n *v1.Node) bool {
			foundPodCount++
			Fail("shouldn't track pods with preferred anti-affinity")
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 0))
	})
	It("should stop tracking pods with required anti-affinity if the pod is deleted", func() {
		pod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
			PodAntiRequirements: []v1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"foo": "bar"},
					},
					TopologyKey: v1.LabelTopologyZone,
				},
			},
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})

		ExpectApplied(ctx, env.Client, pod)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
		foundPodCount := 0
		cluster.ForPodsWithAntiAffinity(func(p *v1.Pod, n *v1.Node) bool {
			foundPodCount++
			Expect(p.Name).To(Equal(pod.Name))
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 1))

		ExpectDeleted(ctx, env.Client, client.Object(pod))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))
		foundPodCount = 0
		cluster.ForPodsWithAntiAffinity(func(p *v1.Pod, n *v1.Node) bool {
			foundPodCount++
			Fail("should not be called as the pod was deleted")
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 0))
	})
	It("should handle events out of order", func() {
		pod := test.UnschedulablePod(test.PodOptions{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.5"),
				}},
			PodAntiRequirements: []v1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"foo": "bar"},
					},
					TopologyKey: v1.LabelTopologyZone,
				},
			},
		})

		node := test.Node(test.NodeOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				v1.LabelInstanceTypeStable:       cloudProvider.InstanceTypes[0].Name,
			}},
			Allocatable: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("4"),
			}})

		ExpectApplied(ctx, env.Client, pod)
		ExpectApplied(ctx, env.Client, node)
		ExpectManualBinding(ctx, env.Client, pod, node)

		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		ExpectReconcileSucceeded(ctx, podController, client.ObjectKeyFromObject(pod))

		// simulate receiving the node deletion before the pod deletion
		ExpectDeleted(ctx, env.Client, node)
		ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

		foundPodCount := 0
		cluster.ForPodsWithAntiAffinity(func(p *v1.Pod, n *v1.Node) bool {
			foundPodCount++
			return true
		})
		Expect(foundPodCount).To(BeNumerically("==", 0))
	})
})

var _ = Describe("Provisioner Spec Updates", func() {
	It("should cause consolidation state to change when a provisioner is updated", func() {
		cluster.SetConsolidated(true)
		fakeClock.Step(time.Minute)
		provisioner.Spec.Consolidation = &v1alpha5.Consolidation{Enabled: ptr.Bool(true)}
		ExpectApplied(ctx, env.Client, provisioner)
		ExpectReconcileSucceeded(ctx, provisionerController, client.ObjectKeyFromObject(provisioner))
		Expect(cluster.Consolidated()).To(BeFalse())
	})
})

var _ = Describe("Cluster State Sync", func() {
	It("should consider the cluster state synced when all nodes are tracked", func() {
		// Deploy 1000 nodes and sync them all with the cluster
		for i := 0; i < 1000; i++ {
			node := test.Node(test.NodeOptions{
				ProviderID: test.RandomProviderID(),
			})
			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		}
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("should consider the cluster state synced when nodes don't have provider id", func() {
		// Deploy 1000 nodes and sync them all with the cluster
		for i := 0; i < 1000; i++ {
			node := test.Node()
			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		}
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("should consider the cluster state synced when nodes register provider id", func() {
		// Deploy 1000 nodes and sync them all with the cluster
		var nodes []*v1.Node
		for i := 0; i < 1000; i++ {
			nodes = append(nodes, test.Node())
			ExpectApplied(ctx, env.Client, nodes[i])
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(nodes[i]))
		}
		Expect(cluster.Synced(ctx)).To(BeTrue())
		for i := 0; i < 1000; i++ {
			nodes[i].Spec.ProviderID = test.RandomProviderID()
			ExpectApplied(ctx, env.Client, nodes[i])
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(nodes[i]))
		}
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("should consider the cluster state synced when all machines are tracked", func() {
		// Deploy 1000 machines and sync them all with the cluster
		for i := 0; i < 1000; i++ {
			machine := test.Machine(v1alpha5.Machine{
				Status: v1alpha5.MachineStatus{
					ProviderID: test.RandomProviderID(),
				},
			})
			ExpectApplied(ctx, env.Client, machine)
			ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		}
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("should consider the cluster state synced when a combination of machines and nodes are tracked", func() {
		// Deploy 250 nodes to the cluster that also have machines
		for i := 0; i < 250; i++ {
			node := test.Node(test.NodeOptions{
				ProviderID: test.RandomProviderID(),
			})
			machine := test.Machine(v1alpha5.Machine{
				Status: v1alpha5.MachineStatus{
					ProviderID: node.Spec.ProviderID,
				},
			})
			ExpectApplied(ctx, env.Client, node, machine)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		}
		// Deploy 250 nodes to the cluster
		for i := 0; i < 250; i++ {
			node := test.Node(test.NodeOptions{
				ProviderID: test.RandomProviderID(),
			})
			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
		}
		// Deploy 500 machines and sync them all with the cluster
		for i := 0; i < 500; i++ {
			machine := test.Machine(v1alpha5.Machine{
				Status: v1alpha5.MachineStatus{
					ProviderID: test.RandomProviderID(),
				},
			})
			ExpectApplied(ctx, env.Client, machine)
			ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		}
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("should consider the cluster state synced when the representation of nodes is the same", func() {
		// Deploy 500 machines to the cluster, apply the linked nodes, but don't sync them
		for i := 0; i < 500; i++ {
			machine := test.Machine(v1alpha5.Machine{
				Status: v1alpha5.MachineStatus{
					ProviderID: test.RandomProviderID(),
				},
			})
			node := test.Node(test.NodeOptions{
				ProviderID: machine.Status.ProviderID,
			})
			ExpectApplied(ctx, env.Client, machine)
			ExpectApplied(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		}
		Expect(cluster.Synced(ctx)).To(BeTrue())
	})
	It("shouldn't consider the cluster state synced if a machine hasn't resolved its provider id", func() {
		// Deploy 1000 machines and sync them all with the cluster
		for i := 0; i < 1000; i++ {
			machine := test.Machine(v1alpha5.Machine{
				Status: v1alpha5.MachineStatus{
					ProviderID: test.RandomProviderID(),
				},
			})
			// One of them doesn't have its providerID
			if i == 900 {
				machine.Status.ProviderID = ""
			}
			ExpectApplied(ctx, env.Client, machine)
			ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
		}
		Expect(cluster.Synced(ctx)).To(BeFalse())
	})
	It("shouldn't consider the cluster state synced if a machine isn't tracked", func() {
		// Deploy 1000 machines and sync them all with the cluster
		for i := 0; i < 1000; i++ {
			machine := test.Machine(v1alpha5.Machine{
				Status: v1alpha5.MachineStatus{
					ProviderID: test.RandomProviderID(),
				},
			})
			ExpectApplied(ctx, env.Client, machine)

			// One of them doesn't get synced with the reconciliation
			if i != 900 {
				ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))
			}
		}
		Expect(cluster.Synced(ctx)).To(BeFalse())
	})
	It("shouldn't consider the cluster state synced if a node isn't tracked", func() {
		// Deploy 1000 nodes and sync them all with the cluster
		for i := 0; i < 1000; i++ {
			node := test.Node(test.NodeOptions{
				ProviderID: test.RandomProviderID(),
			})
			ExpectApplied(ctx, env.Client, node)

			// One of them doesn't get synced with the reconciliation
			if i != 900 {
				ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))
			}
		}
		Expect(cluster.Synced(ctx)).To(BeFalse())
	})
})

func ExpectStateNodeCount(comparator string, count int) int {
	c := 0
	cluster.ForEachNode(func(n *state.Node) bool {
		c++
		return true
	})
	ExpectWithOffset(1, count).To(BeNumerically(comparator, count))
	return c
}

func ExpectStateNodeExistsWithOffset(offset int, node *v1.Node) *state.Node {
	var ret *state.Node
	cluster.ForEachNode(func(n *state.Node) bool {
		if n.Node.Name != node.Name {
			return true
		}
		ret = n.DeepCopy()
		return false
	})
	ExpectWithOffset(offset+1, ret).ToNot(BeNil())
	return ret
}

func ExpectStateNodeExists(node *v1.Node) *state.Node {
	return ExpectStateNodeExistsWithOffset(1, node)
}

func ExpectStateNodeExistsForMachine(machine *v1alpha5.Machine) *state.Node {
	var ret *state.Node
	cluster.ForEachNode(func(n *state.Node) bool {
		if n.Machine.Name != machine.Name {
			return true
		}
		ret = n.DeepCopy()
		return false
	})
	ExpectWithOffset(1, ret).ToNot(BeNil())
	return ret
}

func ExpectStateNodeNotFoundForMachine(machine *v1alpha5.Machine) *state.Node {
	var ret *state.Node
	cluster.ForEachNode(func(n *state.Node) bool {
		if n.Machine.Name != machine.Name {
			return true
		}
		ret = n.DeepCopy()
		return false
	})
	ExpectWithOffset(1, ret).To(BeNil())
	return ret
}

var _ = Describe("DaemonSet Controller", func() {
	It("should not update daemonsetCache when daemonset is created", func() {
		daemonset := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, daemonset)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))
		daemonsetPod := cluster.GetDaemonSetPods(daemonset)
		Expect(daemonsetPod).To(BeNil())
	})
	It("should update daemonsetCache when daemonset pod is created", func() {
		daemonset := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, daemonset)
		daemonsetPod := test.UnschedulablePod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "apps/v1",
							Kind:               "DaemonSet",
							Name:               daemonset.Name,
							UID:                daemonset.UID,
							Controller:         ptr.Bool(true),
							BlockOwnerDeletion: ptr.Bool(true),
						},
					},
				},
			})
		daemonsetPod.Spec = daemonset.Spec.Template.Spec
		ExpectApplied(ctx, env.Client, daemonsetPod)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))

		storedPod := cluster.GetDaemonSetPods(daemonset)
		Expect(storedPod).To(Equal(daemonsetPod))
	})
	It("should not update daemonsetCache with the same daemonset pod spec", func() {
		daemonset := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, daemonset)
		daemonsetPod1 := test.UnschedulablePod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "apps/v1",
							Kind:               "DaemonSet",
							Name:               daemonset.Name,
							UID:                daemonset.UID,
							Controller:         ptr.Bool(true),
							BlockOwnerDeletion: ptr.Bool(true),
						},
					},
				},
			})
		daemonsetPod1.Spec = daemonset.Spec.Template.Spec
		ExpectApplied(ctx, env.Client, daemonsetPod1)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))

		storedPod := cluster.GetDaemonSetPods(daemonset)
		Expect(storedPod).To(Equal(daemonsetPod1))
		daemonsetPod2 := test.UnschedulablePod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "apps/v1",
							Kind:               "DaemonSet",
							Name:               daemonset.Name,
							UID:                daemonset.UID,
							Controller:         ptr.Bool(true),
							BlockOwnerDeletion: ptr.Bool(true),
						},
					},
				},
			})
		daemonsetPod2.Spec = daemonset.Spec.Template.Spec
		ExpectApplied(ctx, env.Client, daemonsetPod1)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))

		storedPod = cluster.GetDaemonSetPods(daemonset)
		Expect(storedPod).To(Equal(daemonsetPod1))
		Expect(storedPod).To(Not(Equal(daemonsetPod2)))
	})
	It("should delete daemonset in cache when daemonset is deleted", func() {
		daemonset := test.DaemonSet(
			test.DaemonSetOptions{PodOptions: test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
		)
		ExpectApplied(ctx, env.Client, daemonset)
		daemonsetPod1 := test.UnschedulablePod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "apps/v1",
							Kind:               "DaemonSet",
							Name:               daemonset.Name,
							UID:                daemonset.UID,
							Controller:         ptr.Bool(true),
							BlockOwnerDeletion: ptr.Bool(true),
						},
					},
				},
			})
		daemonsetPod1.Spec = daemonset.Spec.Template.Spec
		ExpectApplied(ctx, env.Client, daemonsetPod1)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))

		storedPod := cluster.GetDaemonSetPods(daemonset)
		Expect(storedPod).To(Equal(daemonsetPod1))

		ExpectDeleted(ctx, env.Client, daemonset, daemonsetPod1)
		ExpectReconcileSucceeded(ctx, daemonsetController, client.ObjectKeyFromObject(daemonset))

		storedPod = cluster.GetDaemonSetPods(daemonset)
		Expect(storedPod).To(BeNil())
	})
})

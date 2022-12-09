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

package scheduling_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	clock "k8s.io/utils/clock/testing"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis"
	"github.com/aws/karpenter-core/pkg/apis/config/settings"
	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/cloudprovider"
	"github.com/aws/karpenter-core/pkg/cloudprovider/fake"
	"github.com/aws/karpenter-core/pkg/controllers/provisioning/scheduling"
	"github.com/aws/karpenter-core/pkg/controllers/state"
	"github.com/aws/karpenter-core/pkg/operator/controller"
	"github.com/aws/karpenter-core/pkg/operator/scheme"
	pscheduling "github.com/aws/karpenter-core/pkg/scheduling"

	"github.com/aws/karpenter-core/pkg/controllers/provisioning"
	"github.com/aws/karpenter-core/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/ptr"

	. "github.com/aws/karpenter-core/pkg/test/expectations"
)

var ctx context.Context
var provisioner *v1alpha5.Provisioner
var prov *provisioning.Provisioner
var provisioningController controller.Controller
var env *test.Environment
var fakeClock *clock.FakeClock
var cloudProv *fake.CloudProvider
var cluster *state.Cluster
var nodeStateController controller.Controller
var podStateController controller.Controller
var recorder *test.EventRecorder

func TestScheduling(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controllers/Scheduling")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(scheme.Scheme, apis.CRDs...)
	ctx = settings.ToContext(ctx, test.Settings())
	cloudProv = fake.NewCloudProvider()
	instanceTypes, _ := cloudProv.GetInstanceTypes(ctx, nil)
	// set these on the cloud provider so we can manipulate them if needed
	cloudProv.InstanceTypes = instanceTypes
	fakeClock = clock.NewFakeClock(time.Now())
	cluster = state.NewCluster(ctx, fakeClock, env.Client, cloudProv)
	nodeStateController = state.NewNodeController(env.Client, cluster)
	podStateController = state.NewPodController(env.Client, cluster)
	recorder = test.NewEventRecorder()
	prov = provisioning.NewProvisioner(ctx, env.Client, env.KubernetesInterface.CoreV1(), recorder, cloudProv, cluster)
	provisioningController = provisioning.NewController(env.Client, prov, recorder)
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = BeforeEach(func() {
	provisioner = test.Provisioner(test.ProvisionerOptions{Requirements: []v1.NodeSelectorRequirement{{
		Key:      v1alpha5.LabelCapacityType,
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{v1alpha5.CapacityTypeSpot, v1alpha5.CapacityTypeOnDemand},
	}}})
	// reset instance types
	newCP := fake.CloudProvider{}
	cloudProv.InstanceTypes, _ = newCP.GetInstanceTypes(context.Background(), nil)
	cloudProv.CreateCalls = nil
	recorder.Reset()
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
	cluster.Reset(ctx)
})

var _ = Describe("Custom Constraints", func() {
	Context("Provisioner with Labels", func() {
		It("should schedule unconstrained pods that don't have matching node selectors", func() {
			provisioner.Spec.Labels = map[string]string{"test-key": "test-value"}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue("test-key", "test-value"))
		})
		It("should not schedule pods that have conflicting node selectors", func() {
			provisioner.Spec.Labels = map[string]string{"test-key": "test-value"}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeSelector: map[string]string{"test-key": "different-value"}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should not schedule pods that have node selectors with undefined key", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeSelector: map[string]string{"test-key": "test-value"}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should schedule pods that have matching requirements", func() {
			provisioner.Spec.Labels = map[string]string{"test-key": "test-value"}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value", "another-value"}},
				}},
			))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue("test-key", "test-value"))
		})
		It("should not schedule pods that have conflicting requirements", func() {
			provisioner.Spec.Labels = map[string]string{"test-key": "test-value"}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"another-value"}},
				}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
	})
	Context("Well Known Labels", func() {
		It("should use provisioner constraints", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-2"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-2"))
		})
		It("should use node selectors", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeSelector: map[string]string{v1.LabelTopologyZone: "test-zone-2"}},
			))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-2"))
		})
		It("should not schedule nodes with a hostname selector", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeSelector: map[string]string{v1.LabelHostname: "red-node"}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should not schedule the pod if nodeselector unknown", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeSelector: map[string]string{v1.LabelTopologyZone: "unknown"}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should not schedule if node selector outside of provisioner constraints", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeSelector: map[string]string{v1.LabelTopologyZone: "test-zone-2"}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should schedule compatible requirements with Operator=In", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-3"}},
				}},
			))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-3"))
		})
		It("should schedule compatible requirements with Operator=Gt", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{{
				Key: fake.IntegerInstanceLabelKey, Operator: v1.NodeSelectorOpGt, Values: []string{"8"},
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(fake.IntegerInstanceLabelKey, "16"))
		})
		It("should schedule compatible requirements with Operator=Lt", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{{
				Key: fake.IntegerInstanceLabelKey, Operator: v1.NodeSelectorOpLt, Values: []string{"8"},
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(fake.IntegerInstanceLabelKey, "2"))
		})
		It("should not schedule incompatible preferences and requirements with Operator=In", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"unknown"}},
				}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should schedule compatible requirements with Operator=NotIn", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn, Values: []string{"test-zone-1", "test-zone-2", "unknown"}},
				}},
			))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-3"))
		})
		It("should not schedule incompatible preferences and requirements with Operator=NotIn", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3", "unknown"}},
					}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should schedule compatible preferences and requirements with Operator=In", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3", "unknown"}}},
					NodePreferences: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-2", "unknown"}}},
				},
			))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-2"))
		})
		It("should schedule incompatible preferences and requirements with Operator=In", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3", "unknown"}}},
					NodePreferences: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"unknown"}}},
				},
			))[0]
			ExpectScheduled(ctx, env.Client, pod)
		})
		It("should schedule compatible preferences and requirements with Operator=NotIn", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3", "unknown"}}},
					NodePreferences: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn, Values: []string{"test-zone-1", "test-zone-3"}}},
				},
			))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-2"))
		})
		It("should schedule incompatible preferences and requirements with Operator=NotIn", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3", "unknown"}}},
					NodePreferences: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3"}}},
				},
			))[0]
			ExpectScheduled(ctx, env.Client, pod)
		})
		It("should schedule compatible node selectors, preferences and requirements", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{
					NodeSelector: map[string]string{v1.LabelTopologyZone: "test-zone-3"},
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3"}}},
					NodePreferences: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3"}}},
				},
			))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-3"))
		})
		It("should combine multidimensional node selectors, preferences and requirements", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{
					NodeSelector: map[string]string{
						v1.LabelTopologyZone:       "test-zone-3",
						v1.LabelInstanceTypeStable: "arm-instance-type",
					},
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-3"}},
						{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"default-instance-type", "arm-instance-type"}},
					},
					NodePreferences: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn, Values: []string{"unknown"}},
						{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpNotIn, Values: []string{"unknown"}},
					},
				},
			))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-3"))
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelInstanceTypeStable, "arm-instance-type"))
		})
	})
	Context("Constraints Validation", func() {
		It("should not schedule pods that have node selectors with restricted labels", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			for label := range v1alpha5.RestrictedLabels {
				pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
					test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: label, Operator: v1.NodeSelectorOpIn, Values: []string{"test"}},
					}}))[0]
				ExpectNotScheduled(ctx, env.Client, pod)
			}
		})
		It("should not schedule pods that have node selectors with restricted domains", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			for domain := range v1alpha5.RestrictedLabelDomains {
				pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
					test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: domain + "/test", Operator: v1.NodeSelectorOpIn, Values: []string{"test"}},
					}}))[0]
				ExpectNotScheduled(ctx, env.Client, pod)
			}
		})
		It("should schedule pods that have node selectors with label in restricted domains exceptions list", func() {
			var requirements []v1.NodeSelectorRequirement
			for domain := range v1alpha5.LabelDomainExceptions {
				requirements = append(requirements, v1.NodeSelectorRequirement{Key: domain + "/test", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}})
			}
			provisioner.Spec.Requirements = requirements
			ExpectApplied(ctx, env.Client, provisioner)
			for domain := range v1alpha5.LabelDomainExceptions {
				pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())[0]
				node := ExpectScheduled(ctx, env.Client, pod)
				Expect(node.Labels).To(HaveKeyWithValue(domain+"/test", "test-value"))
			}
		})
		It("should schedule pods that have node selectors with label in wellknown label list", func() {
			schedulable := []*v1.Pod{
				// Constrained by zone
				test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.LabelTopologyZone: "test-zone-1"}}),
				// Constrained by instanceType
				test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.LabelInstanceTypeStable: "default-instance-type"}}),
				// Constrained by architecture
				test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.LabelArchStable: "arm64"}}),
				// Constrained by operatingSystem
				test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.LabelOSStable: string(v1.Linux)}}),
				// Constrained by capacity type
				test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1alpha5.LabelCapacityType: "spot"}}),
			}
			ExpectApplied(ctx, env.Client, provisioner)
			for _, pod := range ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, schedulable...) {
				ExpectScheduled(ctx, env.Client, pod)
			}
		})
	})
	Context("Scheduling Logic", func() {
		It("should not schedule pods that have node selectors with In operator and undefined key", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}},
				}}))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should schedule pods that have node selectors with NotIn operator and undefined key", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpNotIn, Values: []string{"test-value"}},
				}}))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).ToNot(HaveKeyWithValue("test-key", "test-value"))
		})
		It("should not schedule pods that have node selectors with Exists operator and undefined key", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpExists},
				}}))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should schedule pods that with DoesNotExists operator and undefined key", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpDoesNotExist},
				}}))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).ToNot(HaveKey("test-key"))
		})
		It("should schedule unconstrained pods that don't have matching node selectors", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue("test-key", "test-value"))
		})
		It("should schedule pods that have node selectors with matching value and In operator", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}},
				}}))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue("test-key", "test-value"))
		})
		It("should not schedule pods that have node selectors with matching value and NotIn operator", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpNotIn, Values: []string{"test-value"}},
				}}))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should schedule the pod with Exists operator and defined key", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpExists},
				}},
			))[0]
			ExpectScheduled(ctx, env.Client, pod)
		})
		It("should not schedule the pod with DoesNotExists operator and defined key", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpDoesNotExist},
				}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should not schedule pods that have node selectors with different value and In operator", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"another-value"}},
				}}))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should schedule pods that have node selectors with different value and NotIn operator", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpNotIn, Values: []string{"another-value"}},
				}}))[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue("test-key", "test-value"))
		})
		It("should schedule compatible pods to the same node", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value", "another-value"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}},
				}}),
				test.UnschedulablePod(test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpNotIn, Values: []string{"another-value"}},
				}}))
			node1 := ExpectScheduled(ctx, env.Client, pods[0])
			node2 := ExpectScheduled(ctx, env.Client, pods[1])
			Expect(node1.Labels).To(HaveKeyWithValue("test-key", "test-value"))
			Expect(node2.Labels).To(HaveKeyWithValue("test-key", "test-value"))
			Expect(node1.Name).To(Equal(node2.Name))
		})
		It("should schedule incompatible pods to the different node", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value", "another-value"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"test-value"}},
				}}),
				test.UnschedulablePod(test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: "test-key", Operator: v1.NodeSelectorOpIn, Values: []string{"another-value"}},
				}}))
			node1 := ExpectScheduled(ctx, env.Client, pods[0])
			node2 := ExpectScheduled(ctx, env.Client, pods[1])
			Expect(node1.Labels).To(HaveKeyWithValue("test-key", "test-value"))
			Expect(node2.Labels).To(HaveKeyWithValue("test-key", "another-value"))
			Expect(node1.Name).ToNot(Equal(node2.Name))
		})
		It("Exists operator should not overwrite the existing value", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
				test.PodOptions{
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"non-existent-zone"}},
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpExists},
					}},
			))[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
	})
})

var _ = Describe("Preferential Fallback", func() {
	Context("Required", func() {
		It("should not relax the final term", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}},
				{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"default-instance-type"}},
			}
			pod := test.UnschedulablePod()
			pod.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{MatchExpressions: []v1.NodeSelectorRequirement{
					{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"invalid"}}, // Should not be relaxed
				}},
			}}}}
			// Don't relax
			ExpectApplied(ctx, env.Client, provisioner)
			pod = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod)[0]
			ExpectNotScheduled(ctx, env.Client, pod)
		})
		It("should relax multiple terms", func() {
			pod := test.UnschedulablePod()
			pod.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
				{MatchExpressions: []v1.NodeSelectorRequirement{
					{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"invalid"}},
				}},
				{MatchExpressions: []v1.NodeSelectorRequirement{
					{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"invalid"}},
				}},
				{MatchExpressions: []v1.NodeSelectorRequirement{
					{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}},
				}},
				{MatchExpressions: []v1.NodeSelectorRequirement{
					{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-2"}}, // OR operator, never get to this one
				}},
			}}}}
			// Success
			ExpectApplied(ctx, env.Client, provisioner)
			pod = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod)[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-1"))
		})
	})
	Context("Preferred", func() {
		It("should relax all terms", func() {
			pod := test.UnschedulablePod()
			pod.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
				{
					Weight: 1, Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"invalid"}},
					}},
				},
				{
					Weight: 1, Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{
						{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"invalid"}},
					}},
				},
			}}}
			// Success
			ExpectApplied(ctx, env.Client, provisioner)
			pod = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod)[0]
			ExpectScheduled(ctx, env.Client, pod)
		})
		It("should relax to use lighter weights", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2"}}}
			pod := test.UnschedulablePod()
			pod.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
				{
					Weight: 100, Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{
						{Key: v1.LabelInstanceTypeStable, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-3"}},
					}},
				},
				{
					Weight: 50, Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-2"}},
					}},
				},
				{
					Weight: 1, Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{ // OR operator, never get to this one
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}},
					}},
				},
			}}}
			// Success
			ExpectApplied(ctx, env.Client, provisioner)
			pod = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod)[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-2"))
		})
		It("should schedule even if preference is conflicting with requirement", func() {
			pod := test.UnschedulablePod()
			pod.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
				{
					Weight: 1, Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn, Values: []string{"test-zone-3"}},
					}},
				},
			},
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{
					{MatchExpressions: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-3"}}, // Should not be relaxed
					}},
				}},
			}}
			// Success
			ExpectApplied(ctx, env.Client, provisioner)
			pod = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod)[0]
			node := ExpectScheduled(ctx, env.Client, pod)
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-3"))
		})
		It("should schedule even if preference requirements are conflicting", func() {
			pod := test.UnschedulablePod(test.PodOptions{NodePreferences: []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"invalid"}},
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpNotIn, Values: []string{"invalid"}},
			}})
			ExpectApplied(ctx, env.Client, provisioner)
			pod = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod)[0]
			ExpectScheduled(ctx, env.Client, pod)
		})
	})
})

var _ = Describe("Topology", func() {
	labels := map[string]string{"test": "test"}

	It("should ignore unknown topology keys", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
			test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: []v1.TopologySpreadConstraint{{
				TopologyKey:       "unknown",
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}},
		))[0]
		ExpectNotScheduled(ctx, env.Client, pod)
	})
	Context("Zonal", func() {
		It("should balance pods across zones (match labels)", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1, 2))
		})
		It("should balance pods across zones (match expressions)", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "test",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"test"},
						},
					},
				},
				MaxSkew: 1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1, 2))
		})
		It("should respect provisioner zonal constraints", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3"}}}
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1, 2))
		})
		It("should respect provisioner zonal constraints (existing pod)", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			// need enough resource requests that the first node we create fills a node and can't act as an in-flight
			// node for the other pods
			rr := v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.1"),
				},
			}
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr,
					NodeSelector: map[string]string{
						v1.LabelTopologyZone: "test-zone-3",
					},
				}))
			ExpectScheduled(ctx, env.Client, pods[0])

			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2"}}}
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, ResourceRequirements: rr, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, ResourceRequirements: rr, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, ResourceRequirements: rr, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, ResourceRequirements: rr, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, ResourceRequirements: rr, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, ResourceRequirements: rr, TopologySpreadConstraints: topology}),
			)
			// we should have unschedulable pods now, the provisioner can only schedule to zone-1/zone-2, but because of the existing
			// pod in zone-3 it can put a max of two per zone before it would violate max skew
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 2, 2))
		})
		It("should schedule to the non-minimum domain if its all that's available", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           5,
			}}
			rr := v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.1"),
				},
			}
			// force this pod onto zone-1
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr, TopologySpreadConstraints: topology}))
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1))

			// force this pod onto zone-2
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-2"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr, TopologySpreadConstraints: topology}))
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1))

			// now only allow scheduling pods on zone-3
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-3"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(10, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr, TopologySpreadConstraints: topology})...,
			)

			// max skew of 5, so test-zone-1/2 will have 1 pod each, test-zone-3 will have 6, and the rest will fail to schedule
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1, 6))
		})
		It("should only schedule to minimum domains if already violating max skew", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			rr := v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.1"),
				},
			}
			createPods := func(count int) []*v1.Pod {
				var pods []*v1.Pod
				for i := 0; i < count; i++ {
					pods = append(pods, test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
						ResourceRequirements: rr, TopologySpreadConstraints: topology}))
				}
				return pods
			}
			// Spread 9 pods
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, createPods(9)...)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(3, 3, 3))

			// Delete pods to create a skew
			for _, pod := range pods {
				node := ExpectScheduled(ctx, env.Client, pod)
				if node.Labels[v1.LabelTopologyZone] != "test-zone-1" {
					ExpectDeleted(ctx, env.Client, pod)
				}
			}
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(3))

			// Create 3 more pods, skew should recover
			_ = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, createPods(3)...)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(3, 1, 2))
		})
		It("should not violate max-skew when unsat = do not schedule", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			rr := v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.1"),
				},
			}
			// force this pod onto zone-1
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr, TopologySpreadConstraints: topology}))
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1))

			// now only allow scheduling pods on zone-2 and zone-3
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-2", "test-zone-3"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(10, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr, TopologySpreadConstraints: topology})...,
			)

			// max skew of 1, so test-zone-2/3 will have 2 nodes each and the rest of the pods will fail to schedule
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 2, 2))
		})
		It("should not violate max-skew when unsat = do not schedule (discover domains)", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			rr := v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.1"),
				},
			}
			// force this pod onto zone-1
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, ResourceRequirements: rr}))

			// now only allow scheduling pods on zone-2 and zone-3
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-2", "test-zone-3"}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(10, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					TopologySpreadConstraints: topology, ResourceRequirements: rr})...,
			)

			// max skew of 1, so test-zone-2/3 will have 2 nodes each and the rest of the pods will fail to schedule since
			// test-zone-1 has 1 pods in it.
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 2, 2))
		})
		It("should only count running/scheduled pods with matching labels scheduled to nodes with a corresponding domain", func() {
			wrongNamespace := test.RandomName()
			firstNode := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.LabelTopologyZone: "test-zone-1"}}})
			secondNode := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.LabelTopologyZone: "test-zone-2"}}})
			thirdNode := test.Node(test.NodeOptions{}) // missing topology domain
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner, firstNode, secondNode, thirdNode, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: wrongNamespace}})
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.Pod(test.PodOptions{NodeName: firstNode.Name}),                                                                                                                         // ignored, missing labels
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}}),                                                                                                    // ignored, pending
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: thirdNode.Name}),                                                                          // ignored, no domain on node
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels, Namespace: wrongNamespace}, NodeName: firstNode.Name}),                                               // ignored, wrong namespace
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels, DeletionTimestamp: &metav1.Time{Time: time.Now().Add(10 * time.Second)}}, NodeName: firstNode.Name}), // ignored, terminating
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: firstNode.Name, Phase: v1.PodFailed}),                                                     // ignored, phase=Failed
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: firstNode.Name, Phase: v1.PodSucceeded}),                                                  // ignored, phase=Succeeded
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: firstNode.Name}),
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: firstNode.Name}),
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: secondNode.Name}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			nodes := v1.NodeList{}
			Expect(env.Client.List(ctx, &nodes)).To(Succeed())
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(2, 2, 1))
		})
		It("should match all pods when labelSelector is not specified", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1))
		})
		It("should handle interdependent selectors", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{TopologySpreadConstraints: topology})...,
			)
			// This is weird, but the topology label selector is used for determining domain counts. The pod that
			// owns the topology is what the spread actually applies to.  In this test case, there are no pods matching
			// the label selector, so the max skew is zero.  This means we can pack all the pods onto the same node since
			// it doesn't violate the topology spread constraint (i.e. adding new pods doesn't increase skew since the
			// pods we are adding don't count toward skew). This behavior is called out at
			// https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/ , though it's not
			// recommended for users.
			nodeNames := sets.NewString()
			for _, p := range pods {
				nodeNames.Insert(p.Spec.NodeName)
			}
			Expect(nodeNames).To(HaveLen(1))
		})
	})

	Context("Hostname", func() {
		It("should balance pods across nodes", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1, 1, 1))
		})
		It("should balance pods on the same hostname up to maxskew", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           4,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(4))
		})
		It("balance multiple deployments with hostname topology spread", func() {
			// Issue #1425
			spreadPod := func(appName string) test.PodOptions {
				return test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": appName,
						},
					},
					TopologySpreadConstraints: []v1.TopologySpreadConstraint{
						{
							MaxSkew:           1,
							TopologyKey:       v1.LabelHostname,
							WhenUnsatisfiable: v1.DoNotSchedule,
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"app": appName},
							},
						},
					},
				}
			}

			ExpectApplied(ctx, env.Client, provisioner)
			scheduled := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(spreadPod("app1")), test.UnschedulablePod(spreadPod("app1")),
				test.UnschedulablePod(spreadPod("app2")), test.UnschedulablePod(spreadPod("app2")))

			for _, p := range scheduled {
				ExpectScheduled(ctx, env.Client, p)
			}
			nodes := v1.NodeList{}
			Expect(env.Client.List(ctx, &nodes)).To(Succeed())
			// this wasn't part of #1425, but ensures that we launch the minimum number of nodes
			Expect(nodes.Items).To(HaveLen(2))
		})
		It("balance multiple deployments with hostname topology spread & varying arch", func() {
			// Issue #1425
			spreadPod := func(appName, arch string) test.PodOptions {
				return test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app": appName,
						},
					},
					NodeRequirements: []v1.NodeSelectorRequirement{
						{
							Key:      v1.LabelArchStable,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{arch},
						},
					},
					TopologySpreadConstraints: []v1.TopologySpreadConstraint{
						{
							MaxSkew:           1,
							TopologyKey:       v1.LabelHostname,
							WhenUnsatisfiable: v1.DoNotSchedule,
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"app": appName},
							},
						},
					},
				}
			}

			ExpectApplied(ctx, env.Client, provisioner)
			scheduled := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(spreadPod("app1", v1alpha5.ArchitectureAmd64)), test.UnschedulablePod(spreadPod("app1", v1alpha5.ArchitectureAmd64)),
				test.UnschedulablePod(spreadPod("app2", v1alpha5.ArchitectureArm64)), test.UnschedulablePod(spreadPod("app2", v1alpha5.ArchitectureArm64)))

			for _, p := range scheduled {
				ExpectScheduled(ctx, env.Client, p)
			}
			nodes := v1.NodeList{}
			Expect(env.Client.List(ctx, &nodes)).To(Succeed())
			// same test as the previous one, but now the architectures are different so we need four nodes in total
			Expect(nodes.Items).To(HaveLen(4))
		})
	})

	Context("CapacityType", func() {
		It("should balance pods across capacity types", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(2, 2))
		})
		It("should respect provisioner capacity type constraints", func() {
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{v1alpha5.CapacityTypeSpot, v1alpha5.CapacityTypeOnDemand}}}
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(2, 2))
		})
		It("should not violate max-skew when unsat = do not schedule (capacity type)", func() {
			// this test can pass in a flaky manner if we don't restrict our min domain selection to valid choices
			// per the provisioner spec
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			rr := v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.1"),
				},
			}
			// force this pod onto spot
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{v1alpha5.CapacityTypeSpot}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr, TopologySpreadConstraints: topology}))
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1))

			// now only allow scheduling pods on on-demand
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{v1alpha5.CapacityTypeOnDemand}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr, TopologySpreadConstraints: topology})...,
			)

			// max skew of 1, so on-demand will have 2 pods and the rest of the pods will fail to schedule
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 2))
		})
		It("should violate max-skew when unsat = schedule anyway (capacity type)", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.ScheduleAnyway,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			rr := v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("1.1"),
				},
			}
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{v1alpha5.CapacityTypeSpot}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr, TopologySpreadConstraints: topology}))
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1))

			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{v1alpha5.CapacityTypeOnDemand}}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels},
					ResourceRequirements: rr, TopologySpreadConstraints: topology})...,
			)

			// max skew of 1, on-demand will end up with 5 pods even though spot has a single pod
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 5))
		})
		It("should only count running/scheduled pods with matching labels scheduled to nodes with a corresponding domain", func() {
			wrongNamespace := test.RandomName()
			firstNode := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1alpha5.LabelCapacityType: v1alpha5.CapacityTypeSpot}}})
			secondNode := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1alpha5.LabelCapacityType: v1alpha5.CapacityTypeOnDemand}}})
			thirdNode := test.Node(test.NodeOptions{}) // missing topology capacity type
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner, firstNode, secondNode, thirdNode, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: wrongNamespace}})
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.Pod(test.PodOptions{NodeName: firstNode.Name}),                                                                                                                         // ignored, missing labels
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}}),                                                                                                    // ignored, pending
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: thirdNode.Name}),                                                                          // ignored, no domain on node
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels, Namespace: wrongNamespace}, NodeName: firstNode.Name}),                                               // ignored, wrong namespace
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels, DeletionTimestamp: &metav1.Time{Time: time.Now().Add(10 * time.Second)}}, NodeName: firstNode.Name}), // ignored, terminating
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: firstNode.Name, Phase: v1.PodFailed}),                                                     // ignored, phase=Failed
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: firstNode.Name, Phase: v1.PodSucceeded}),                                                  // ignored, phase=Succeeded
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: firstNode.Name}),
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: firstNode.Name}),
				test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, NodeName: secondNode.Name}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			nodes := v1.NodeList{}
			Expect(env.Client.List(ctx, &nodes)).To(Succeed())
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(2, 3))
		})
		It("should match all pods when labelSelector is not specified", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1))
		})
		It("should handle interdependent selectors", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{TopologySpreadConstraints: topology})...,
			)
			// This is weird, but the topology label selector is used for determining domain counts. The pod that
			// owns the topology is what the spread actually applies to.  In this test case, there are no pods matching
			// the label selector, so the max skew is zero.  This means we can pack all the pods onto the same node since
			// it doesn't violate the topology spread constraint (i.e. adding new pods doesn't increase skew since the
			// pods we are adding don't count toward skew). This behavior is called out at
			// https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/ , though it's not
			// recommended for users.
			nodeNames := sets.NewString()
			for _, p := range pods {
				nodeNames.Insert(p.Spec.NodeName)
			}
			Expect(nodeNames).To(HaveLen(1))
		})
		It("should balance pods across capacity-types (node required affinity constrained)", func() {
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, MakePods(1, test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				NodeRequirements: []v1.NodeSelectorRequirement{
					// launch this on-demand pod in zone-1
					{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}},
					{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{"on-demand"}},
				},
			})...)
			ExpectScheduled(ctx, env.Client, pod[0])

			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			// Try to run 5 pods, with a node selector restricted to test-zone-2, they should all schedule on the same
			// spot node. This doesn't violate the max-skew of 1 as the node selector requirement here excludes the
			// existing on-demand pod from counting within this topology.
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					// limit our provisioner to only creating spot nodes
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-2"}},
						{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{"spot"}},
					},
					TopologySpreadConstraints: topology,
				})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 5))
		})
		It("should balance pods across capacity-types (no constraints)", func() {
			rr := v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
			}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(test.PodOptions{
				ObjectMeta:   metav1.ObjectMeta{Labels: labels},
				NodeSelector: map[string]string{v1.LabelInstanceTypeStable: "single-pod-instance-type"},
				NodeRequirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1alpha5.LabelCapacityType,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"on-demand"},
					},
				},
			}))[0]

			ExpectScheduled(ctx, env.Client, pod)

			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			// limit our provisioner to only creating spot nodes
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{"spot"}},
			}

			// since there is no node selector on this pod, the topology can see the single on-demand node that already
			// exists and that limits us to scheduling 2 more spot pods before we would violate max-skew
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{
					ObjectMeta:                metav1.ObjectMeta{Labels: labels},
					ResourceRequirements:      rr,
					TopologySpreadConstraints: topology,
				})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 2))
		})
		It("should balance pods across arch (no constraints)", func() {
			rr := v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
			}
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(test.PodOptions{
				ObjectMeta:   metav1.ObjectMeta{Labels: labels},
				NodeSelector: map[string]string{v1.LabelInstanceTypeStable: "single-pod-instance-type"},
				NodeRequirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelArchStable,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"amd64"},
					},
				},
			}))

			ExpectScheduled(ctx, env.Client, pod[0])

			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelArchStable,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			// limit our provisioner to only creating arm64 nodes
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelArchStable, Operator: v1.NodeSelectorOpIn, Values: []string{"arm64"}}}

			// since there is no node selector on this pod, the topology can see the single arm64 node that already
			// exists and that limits us to scheduling 2 more spot pods before we would violate max-skew
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{
					ObjectMeta:                metav1.ObjectMeta{Labels: labels},
					ResourceRequirements:      rr,
					TopologySpreadConstraints: topology,
				})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 2))
		})
	})

	Context("Combined Hostname and Zonal Topology", func() {
		It("should spread pods while respecting both constraints", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}, {
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           3,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(2, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 3)))

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(3, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(2, 2, 1))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 3)))

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(4, 3, 3))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 3)))

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(11, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(7, 7, 7))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 3)))
		})
		It("should balance pods across provisioner requirements", func() {
			spotProv := test.Provisioner(test.ProvisionerOptions{
				Requirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1alpha5.LabelCapacityType,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"spot"},
					},
					{
						Key:      "capacity.spread.4-1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"2", "3", "4", "5"},
					},
				},
			})
			onDemandProv := test.Provisioner(test.ProvisionerOptions{
				Requirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1alpha5.LabelCapacityType,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"on-demand"},
					},
					{
						Key:      "capacity.spread.4-1",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"1"},
					},
				},
			})

			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       "capacity.spread.4-1",
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, spotProv, onDemandProv)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, MakePods(20, test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: topology,
			})...)
			for _, p := range pods {
				ExpectScheduled(ctx, env.Client, p)
			}

			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(4, 4, 4, 4, 4))
			// due to the spread across provisioners, we've forced a 4:1 spot to on-demand spread
			ExpectSkew(ctx, env.Client, "default", &v1.TopologySpreadConstraint{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}).To(ConsistOf(4, 16))
		})
	})

	Context("Combined Hostname and Zonal Topology", func() {
		It("should spread pods while respecting both constraints", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}, {
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.ScheduleAnyway,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2"}}}

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(10, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)

			// should get one pod per zone, can't schedule to test-zone-3
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1))
			// and one pod per node
			ExpectSkew(ctx, env.Client, "default", &topology[1]).To(ConsistOf(1, 1))
		})
	})

	Context("Combined Hostname and Capacity Type Topology", func() {
		It("should spread pods while respecting both constraints", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}, {
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           3,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(2, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 3)))

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(3, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(3, 2))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 3)))

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(5, 5))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 3)))

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(11, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(11, 10))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 3)))
		})
	})

	Context("Combined Zonal and Capacity Type Topology", func() {
		It("should spread pods while respecting both constraints", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}, {
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(2, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).ToNot(ContainElements(BeNumerically(">", 1)))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 1)))

			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(3, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).ToNot(ContainElements(BeNumerically(">", 3)))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 2)))

			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).ToNot(ContainElements(BeNumerically(">", 5)))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 4)))

			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(11, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).ToNot(ContainElements(BeNumerically(">", 11)))
			ExpectSkew(ctx, env.Client, "default", &topology[1]).ToNot(ContainElements(BeNumerically(">", 7)))
		})
	})

	Context("Combined Hostname, Zonal, and Capacity Type Topology", func() {
		It("should spread pods while respecting all constraints", func() {
			// ensure we've got an instance type for every zone/capacity-type pair
			cloudProv.InstanceTypes = fake.InstanceTypesAssorted()
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}, {
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           2,
			}, {
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           3,
			}}

			// add varying numbers of pods, checking after each scheduling to ensure that our max required max skew
			// has not been violated for each constraint
			for i := 1; i < 15; i++ {
				pods := MakePods(i, test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology})
				ExpectApplied(ctx, env.Client, provisioner)
				ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
				ExpectMaxSkew(ctx, env.Client, "default", &topology[0]).To(BeNumerically("<=", 1))
				ExpectMaxSkew(ctx, env.Client, "default", &topology[1]).To(BeNumerically("<=", 2))
				ExpectMaxSkew(ctx, env.Client, "default", &topology[2]).To(BeNumerically("<=", 3))
				for _, pod := range pods {
					ExpectScheduled(ctx, env.Client, pod)
				}
			}
		})
	})

	// https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/#interaction-with-node-affinity-and-node-selectors
	Context("Combined Zonal Topology and Node Affinity", func() {
		It("should limit spread options by nodeSelector", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				append(
					MakePods(5, test.PodOptions{
						ObjectMeta:                metav1.ObjectMeta{Labels: labels},
						TopologySpreadConstraints: topology,
						NodeSelector:              map[string]string{v1.LabelTopologyZone: "test-zone-1"},
					}),
					MakePods(10, test.PodOptions{
						ObjectMeta:                metav1.ObjectMeta{Labels: labels},
						TopologySpreadConstraints: topology,
						NodeSelector:              map[string]string{v1.LabelTopologyZone: "test-zone-2"},
					})...,
				)...,
			)
			// we limit the zones of each pod via node selectors, which causes the topology spreads to only consider
			// the single zone as the only valid domain for the topology spread allowing us to schedule multiple pods per domain
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(5, 10))
		})
		It("should limit spread options by node requirements", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(10, test.PodOptions{
					ObjectMeta:                metav1.ObjectMeta{Labels: labels},
					TopologySpreadConstraints: topology,
					NodeRequirements: []v1.NodeSelectorRequirement{
						{
							Key:      v1.LabelTopologyZone,
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{"test-zone-1", "test-zone-2"},
						},
					},
				})...)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(5, 5))
		})
		It("should limit spread options by node affinity", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(6, test.PodOptions{
					ObjectMeta:                metav1.ObjectMeta{Labels: labels},
					TopologySpreadConstraints: topology,
					NodeRequirements: []v1.NodeSelectorRequirement{{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{
						"test-zone-1", "test-zone-2",
					}}},
				})...)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(3, 3))

			// open the provisioner back to up so it can see all zones again
			provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{
				{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1", "test-zone-2", "test-zone-3"}}}

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, MakePods(1, test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: topology,
				NodeRequirements: []v1.NodeSelectorRequirement{{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{
					"test-zone-2", "test-zone-3",
				}}},
			})...)

			// it will schedule on the currently empty zone-3 even though max-skew is violated as it improves max-skew
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(3, 3, 1))

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{
					ObjectMeta:                metav1.ObjectMeta{Labels: labels},
					TopologySpreadConstraints: topology,
				})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(4, 4, 4))
		})
	})

	// https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/#interaction-with-node-affinity-and-node-selectors
	Context("Combined Capacity Type Topology and Node Affinity", func() {
		It("should limit spread options by nodeSelector", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.ScheduleAnyway,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				append(
					MakePods(5, test.PodOptions{
						ObjectMeta:                metav1.ObjectMeta{Labels: labels},
						TopologySpreadConstraints: topology,
						NodeSelector:              map[string]string{v1alpha5.LabelCapacityType: v1alpha5.CapacityTypeSpot},
					}),
					MakePods(5, test.PodOptions{
						ObjectMeta:                metav1.ObjectMeta{Labels: labels},
						TopologySpreadConstraints: topology,
						NodeSelector:              map[string]string{v1alpha5.LabelCapacityType: v1alpha5.CapacityTypeOnDemand},
					})...,
				)...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(5, 5))
		})
		It("should limit spread options by node affinity (capacity type)", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1alpha5.LabelCapacityType,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			// need to limit the rules to spot or else it will know that on-demand has 0 pods and won't violate the max-skew
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(3, test.PodOptions{
					ObjectMeta:                metav1.ObjectMeta{Labels: labels},
					TopologySpreadConstraints: topology,
					NodeRequirements: []v1.NodeSelectorRequirement{
						{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{v1alpha5.CapacityTypeSpot}},
					},
				})...)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(3))

			// open the rules back to up so it can see all capacity types
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, MakePods(1, test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: topology,
				NodeRequirements: []v1.NodeSelectorRequirement{
					{Key: v1alpha5.LabelCapacityType, Operator: v1.NodeSelectorOpIn, Values: []string{v1alpha5.CapacityTypeOnDemand, v1alpha5.CapacityTypeSpot}},
				},
			})...)

			// it will schedule on the currently empty on-demand even though max-skew is violated as it improves max-skew
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(3, 1))

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				MakePods(5, test.PodOptions{
					ObjectMeta:                metav1.ObjectMeta{Labels: labels},
					TopologySpreadConstraints: topology,
				})...,
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(5, 4))
		})
	})

	Context("Pod Affinity/Anti-Affinity", func() {
		It("should schedule a pod with empty pod affinity and anti-affinity", func() {
			ExpectApplied(ctx, env.Client)
			ExpectApplied(ctx, env.Client, provisioner)
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(test.PodOptions{
				PodRequirements:     []v1.PodAffinityTerm{},
				PodAntiRequirements: []v1.PodAffinityTerm{},
			}))[0]
			ExpectScheduled(ctx, env.Client, pod)
		})
		It("should respect pod affinity (hostname)", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			affLabels := map[string]string{"security": "s2"}

			affPod1 := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels}})
			// affPod2 will try to get scheduled with affPod1
			affPod2 := test.UnschedulablePod(test.PodOptions{PodRequirements: []v1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: affLabels,
				},
				TopologyKey: v1.LabelHostname,
			}}})

			var pods []*v1.Pod
			pods = append(pods, MakePods(10, test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: topology,
			})...)
			pods = append(pods, affPod1)
			pods = append(pods, affPod2)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			n1 := ExpectScheduled(ctx, env.Client, affPod1)
			n2 := ExpectScheduled(ctx, env.Client, affPod2)
			// should be scheduled on the same node
			Expect(n1.Name).To(Equal(n2.Name))
		})
		It("should respect pod affinity (arch)", func() {
			affLabels := map[string]string{"security": "s2"}
			tsc := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: affLabels},
				MaxSkew:           1,
			}}

			affPod1 := test.UnschedulablePod(test.PodOptions{
				TopologySpreadConstraints: tsc,
				ObjectMeta:                metav1.ObjectMeta{Labels: affLabels},
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
				},
				NodeSelector: map[string]string{
					v1.LabelArchStable: "arm64",
				}})
			// affPod2 will try to get scheduled with affPod1
			affPod2 := test.UnschedulablePod(test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: affLabels},
				TopologySpreadConstraints: tsc,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
				},
				PodRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelArchStable,
				}}})

			pods := []*v1.Pod{affPod1, affPod2}

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			n1 := ExpectScheduled(ctx, env.Client, affPod1)
			n2 := ExpectScheduled(ctx, env.Client, affPod2)
			// should be scheduled on a node with the same arch
			Expect(n1.Labels[v1.LabelArchStable]).To(Equal(n2.Labels[v1.LabelArchStable]))
			// but due to TSC, not on the same node
			Expect(n1.Name).ToNot(Equal(n2.Name))
		})
		It("should respect self pod affinity (hostname)", func() {
			affLabels := map[string]string{"security": "s2"}

			pods := MakePods(3, test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: affLabels,
				},
				PodRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelHostname,
				}},
			})

			ExpectApplied(ctx, env.Client, provisioner)
			pods = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			nodeNames := map[string]struct{}{}
			for _, p := range pods {
				n := ExpectScheduled(ctx, env.Client, p)
				nodeNames[n.Name] = struct{}{}
			}
			Expect(len(nodeNames)).To(Equal(1))
		})
		It("should respect self pod affinity for first empty topology domain only (hostname)", func() {
			affLabels := map[string]string{"security": "s2"}
			createPods := func() []*v1.Pod {
				return MakePods(10, test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{
						Labels: affLabels,
					},
					PodRequirements: []v1.PodAffinityTerm{{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: affLabels,
						},
						TopologyKey: v1.LabelHostname,
					}},
				})
			}
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, createPods()...)
			nodeNames := map[string]struct{}{}
			unscheduledCount := 0
			scheduledCount := 0
			for _, p := range pods {
				p = ExpectPodExists(ctx, env.Client, p.Name, p.Namespace)
				if p.Spec.NodeName == "" {
					unscheduledCount++
				} else {
					nodeNames[p.Spec.NodeName] = struct{}{}
					scheduledCount++
				}
			}
			// the node can only hold 5 pods, so we should get a single node with 5 pods and 5 unschedulable pods from that batch
			Expect(len(nodeNames)).To(Equal(1))
			Expect(scheduledCount).To(BeNumerically("==", 5))
			Expect(unscheduledCount).To(BeNumerically("==", 5))

			// and pods in a different batch should not schedule as well even if the node is not ready yet
			pods = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, createPods()...)
			for _, p := range pods {
				ExpectNotScheduled(ctx, env.Client, p)
			}
		})
		It("should respect self pod affinity for first empty topology domain only (hostname/constrained zones)", func() {
			affLabels := map[string]string{"security": "s2"}
			// put one pod in test-zone-1, this does affect pod affinity even though we have different node selectors.
			// The node selector and required node affinity restrictions to topology counting only apply to topology spread.
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: affLabels,
				},
				NodeSelector: map[string]string{
					v1.LabelTopologyZone: "test-zone-1",
				},
				PodRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelHostname,
				}},
			}))

			pods := MakePods(10, test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: affLabels,
				},
				NodeRequirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelTopologyZone,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"test-zone-2", "test-zone-3"},
					},
				},
				PodRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelHostname,
				}},
			})
			pods = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			for _, p := range pods {
				// none of this should schedule
				ExpectNotScheduled(ctx, env.Client, p)
			}
		})
		It("should respect self pod affinity (zone)", func() {
			affLabels := map[string]string{"security": "s2"}

			pods := MakePods(3, test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: affLabels,
				},
				PodRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelTopologyZone,
				}},
			})

			ExpectApplied(ctx, env.Client, provisioner)
			pods = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			nodeNames := map[string]struct{}{}
			for _, p := range pods {
				n := ExpectScheduled(ctx, env.Client, p)
				nodeNames[n.Name] = struct{}{}
			}
			Expect(len(nodeNames)).To(Equal(1))
		})
		It("should respect self pod affinity (zone w/ constraint)", func() {
			affLabels := map[string]string{"security": "s2"}
			// the pod needs to provide it's own zonal affinity, but we further limit it to only being on test-zone-3
			pods := MakePods(3, test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: affLabels,
				},
				PodRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelTopologyZone,
				}},
				NodeRequirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelTopologyZone,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"test-zone-3"},
					},
				},
			})
			ExpectApplied(ctx, env.Client, provisioner)
			pods = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			nodeNames := map[string]struct{}{}
			for _, p := range pods {
				n := ExpectScheduled(ctx, env.Client, p)
				nodeNames[n.Name] = struct{}{}
				Expect(n.Labels[v1.LabelTopologyZone]).To(Equal("test-zone-3"))
			}
			Expect(len(nodeNames)).To(Equal(1))
		})
		It("should allow violation of preferred pod affinity", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			affPod2 := test.UnschedulablePod(test.PodOptions{PodPreferences: []v1.WeightedPodAffinityTerm{{
				Weight: 50,
				PodAffinityTerm: v1.PodAffinityTerm{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"security": "s2"},
					},
					TopologyKey: v1.LabelHostname,
				},
			}}})

			var pods []*v1.Pod
			pods = append(pods, MakePods(10, test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: topology,
			})...)

			pods = append(pods, affPod2)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			// should be scheduled as the pod it has affinity to doesn't exist, but it's only a preference and not a
			// hard constraints
			ExpectScheduled(ctx, env.Client, affPod2)

		})
		It("should allow violation of preferred pod anti-affinity", func() {
			affPods := MakePods(10, test.PodOptions{PodAntiPreferences: []v1.WeightedPodAffinityTerm{
				{
					Weight: 50,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: labels,
						},
						TopologyKey: v1.LabelTopologyZone,
					},
				},
			}})

			var pods []*v1.Pod
			pods = append(pods, MakePods(3, test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: []v1.TopologySpreadConstraint{{
					TopologyKey:       v1.LabelTopologyZone,
					WhenUnsatisfiable: v1.DoNotSchedule,
					LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
					MaxSkew:           1,
				}},
			})...)

			pods = append(pods, affPods...)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			for _, aff := range affPods {
				ExpectScheduled(ctx, env.Client, aff)
			}

		})
		It("should separate nodes using simple pod anti-affinity on hostname", func() {
			affLabels := map[string]string{"security": "s2"}
			// pod affinity/anti-affinity are bidirectional, so run this a few times to ensure we handle it regardless
			// of pod scheduling order
			ExpectApplied(ctx, env.Client, provisioner)
			for i := 0; i < 10; i++ {
				affPod1 := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels}})
				// affPod2 will avoid affPod1
				affPod2 := test.UnschedulablePod(test.PodOptions{PodAntiRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelHostname,
				}}})

				ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, affPod2, affPod1)
				n1 := ExpectScheduled(ctx, env.Client, affPod1)
				n2 := ExpectScheduled(ctx, env.Client, affPod2)
				// should not be scheduled on the same node
				Expect(n1.Name).ToNot(Equal(n2.Name))
			}
		})
		It("should not violate pod anti-affinity on zone", func() {
			affLabels := map[string]string{"security": "s2"}
			zone1Pod := test.UnschedulablePod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: affLabels},
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
				},
				NodeSelector: map[string]string{v1.LabelTopologyZone: "test-zone-1"}})
			zone2Pod := test.UnschedulablePod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: affLabels},
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
				},
				NodeSelector: map[string]string{v1.LabelTopologyZone: "test-zone-2"}})
			zone3Pod := test.UnschedulablePod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: affLabels},
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
				},
				NodeSelector: map[string]string{v1.LabelTopologyZone: "test-zone-3"}})

			affPod := test.UnschedulablePod(test.PodOptions{
				PodAntiRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelTopologyZone,
				}}})

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, zone1Pod, zone2Pod, zone3Pod, affPod)
			// the three larger zone specific pods should get scheduled first due to first fit descending onto one
			// node per zone.
			ExpectScheduled(ctx, env.Client, zone1Pod)
			ExpectScheduled(ctx, env.Client, zone2Pod)
			ExpectScheduled(ctx, env.Client, zone3Pod)
			// the pod with anti-affinity
			ExpectNotScheduled(ctx, env.Client, affPod)
		})
		It("should not violate pod anti-affinity on zone (other schedules first)", func() {
			affLabels := map[string]string{"security": "s2"}
			pod := test.UnschedulablePod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: affLabels},
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
				}})
			affPod := test.UnschedulablePod(test.PodOptions{
				PodAntiRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelTopologyZone,
				}}})

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod, affPod)
			// the pod we need to avoid schedules first, but we don't know where.
			ExpectScheduled(ctx, env.Client, pod)
			// the pod with anti-affinity
			ExpectNotScheduled(ctx, env.Client, affPod)
		})
		It("should not violate pod anti-affinity (arch)", func() {
			affLabels := map[string]string{"security": "s2"}
			tsc := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: affLabels},
				MaxSkew:           1,
			}}

			affPod1 := test.UnschedulablePod(test.PodOptions{
				TopologySpreadConstraints: tsc,
				ObjectMeta:                metav1.ObjectMeta{Labels: affLabels},
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
				},
				NodeSelector: map[string]string{
					v1.LabelArchStable: "arm64",
				}})

			// affPod2 will try to get scheduled on a node with a different archi from affPod1. Due to resource
			// requests we try to schedule it last
			affPod2 := test.UnschedulablePod(test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: affLabels},
				TopologySpreadConstraints: tsc,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
				},
				PodAntiRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelArchStable,
				}}})

			pods := []*v1.Pod{affPod1, affPod2}

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			n1 := ExpectScheduled(ctx, env.Client, affPod1)
			n2 := ExpectScheduled(ctx, env.Client, affPod2)
			// should not be scheduled on nodes with the same arch
			Expect(n1.Labels[v1.LabelArchStable]).ToNot(Equal(n2.Labels[v1.LabelArchStable]))
		})
		It("should violate preferred pod anti-affinity on zone (inverse)", func() {
			affLabels := map[string]string{"security": "s2"}
			anti := []v1.WeightedPodAffinityTerm{
				{
					Weight: 10,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: affLabels,
						},
						TopologyKey: v1.LabelTopologyZone,
					},
				},
			}
			rr := v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
			}
			zone1Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiPreferences:   anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-1"}})
			zone2Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiPreferences:   anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-2"}})
			zone3Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiPreferences:   anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-3"}})

			affPod := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels}})

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, zone1Pod, zone2Pod, zone3Pod, affPod)
			// three pods with anti-affinity will schedule first due to first fit-descending
			ExpectScheduled(ctx, env.Client, zone1Pod)
			ExpectScheduled(ctx, env.Client, zone2Pod)
			ExpectScheduled(ctx, env.Client, zone3Pod)
			// the anti-affinity was a preference, so this can schedule
			ExpectScheduled(ctx, env.Client, affPod)
		})
		It("should not violate pod anti-affinity on zone (inverse)", func() {
			affLabels := map[string]string{"security": "s2"}
			anti := []v1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: affLabels,
				},
				TopologyKey: v1.LabelTopologyZone,
			}}
			rr := v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
			}
			zone1Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiRequirements:  anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-1"}})
			zone2Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiRequirements:  anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-2"}})
			zone3Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiRequirements:  anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-3"}})

			affPod := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels}})

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, zone1Pod, zone2Pod, zone3Pod, affPod)
			// three pods with anti-affinity will schedule first due to first fit-descending
			ExpectScheduled(ctx, env.Client, zone1Pod)
			ExpectScheduled(ctx, env.Client, zone2Pod)
			ExpectScheduled(ctx, env.Client, zone3Pod)
			// this pod with no anti-affinity rules can't schedule. It has no anti-affinity rules, but every zone has a
			// pod with anti-affinity rules that prevent it from scheduling
			ExpectNotScheduled(ctx, env.Client, affPod)
		})
		It("should not violate pod anti-affinity on zone (Schrödinger)", func() {
			affLabels := map[string]string{"security": "s2"}
			anti := []v1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: affLabels,
				},
				TopologyKey: v1.LabelTopologyZone,
			}}
			zoneAnywherePod := test.UnschedulablePod(test.PodOptions{
				PodAntiRequirements: anti,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
				},
			})

			affPod := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels}})

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, zoneAnywherePod, affPod)
			// the pod with anti-affinity will schedule first due to first fit-descending, but we don't know which zone it landed in
			node1 := ExpectScheduled(ctx, env.Client, zoneAnywherePod)

			// this pod cannot schedule since the pod with anti-affinity could potentially be in any zone
			affPod = ExpectNotScheduled(ctx, env.Client, affPod)

			// a second batching will now allow the pod to schedule as the zoneAnywherePod has been committed to a zone
			// by the actual node creation
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, affPod)
			node2 := ExpectScheduled(ctx, env.Client, affPod)
			Expect(node1.Labels[v1.LabelTopologyZone]).ToNot(Equal(node2.Labels[v1.LabelTopologyZone]))

		})
		It("should not violate pod anti-affinity on zone (inverse w/existing nodes)", func() {
			affLabels := map[string]string{"security": "s2"}
			anti := []v1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: affLabels,
				},
				TopologyKey: v1.LabelTopologyZone,
			}}
			rr := v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
			}
			zone1Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiRequirements:  anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-1"}})
			zone2Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiRequirements:  anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-2"}})
			zone3Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiRequirements:  anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-3"}})

			affPod := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels}})

			// provision these so we get three nodes that exist in the cluster with anti-affinity to a pod that we will
			// then try to schedule
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, zone1Pod, zone2Pod, zone3Pod)
			node1 := ExpectScheduled(ctx, env.Client, zone1Pod)
			node2 := ExpectScheduled(ctx, env.Client, zone2Pod)
			node3 := ExpectScheduled(ctx, env.Client, zone3Pod)

			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node2))
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node3))
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(zone1Pod))
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(zone2Pod))
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(zone3Pod))

			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(zone1Pod))
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(zone2Pod))
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(zone3Pod))

			// this pod with no anti-affinity rules can't schedule. It has no anti-affinity rules, but every zone has an
			// existing pod (not from this batch) with anti-affinity rules that prevent it from scheduling
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, affPod)
			ExpectNotScheduled(ctx, env.Client, affPod)
		})
		It("should violate preferred pod anti-affinity on zone (inverse w/existing nodes)", func() {
			affLabels := map[string]string{"security": "s2"}
			anti := []v1.WeightedPodAffinityTerm{
				{
					Weight: 10,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: affLabels,
						},
						TopologyKey: v1.LabelTopologyZone,
					},
				},
			}
			rr := v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
			}
			zone1Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiPreferences:   anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-1"}})
			zone2Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiPreferences:   anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-2"}})
			zone3Pod := test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: rr,
				PodAntiPreferences:   anti,
				NodeSelector:         map[string]string{v1.LabelTopologyZone: "test-zone-3"}})

			affPod := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels}})

			// provision these so we get three nodes that exist in the cluster with anti-affinity to a pod that we will
			// then try to schedule
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, zone1Pod, zone2Pod, zone3Pod)
			node1 := ExpectScheduled(ctx, env.Client, zone1Pod)
			node2 := ExpectScheduled(ctx, env.Client, zone2Pod)
			node3 := ExpectScheduled(ctx, env.Client, zone3Pod)

			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node2))
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node3))
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(zone1Pod))
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(zone2Pod))
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(zone3Pod))

			// this pod with no anti-affinity rules can schedule, though it couldn't if the anti-affinity were required
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, affPod)
			ExpectScheduled(ctx, env.Client, affPod)
		})
		It("should allow violation of a pod affinity preference with a conflicting required constraint", func() {
			affLabels := map[string]string{"security": "s2"}
			constraint := v1.TopologySpreadConstraint{
				MaxSkew:           1,
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: labels,
				},
			}
			affPod1 := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels}})
			affPods := MakePods(3, test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				// limit these pods to one per host
				TopologySpreadConstraints: []v1.TopologySpreadConstraint{constraint},
				// with a preference to the other pod
				PodPreferences: []v1.WeightedPodAffinityTerm{{
					Weight: 50,
					PodAffinityTerm: v1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: affLabels,
						},
						TopologyKey: v1.LabelHostname,
					},
				}}})
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, append(affPods, affPod1)...)
			// all pods should be scheduled since the affinity term is just a preference
			for _, pod := range pods {
				ExpectScheduled(ctx, env.Client, pod)
			}
			// and we'll get three nodes due to the topology spread
			ExpectSkew(ctx, env.Client, "", &constraint).To(ConsistOf(1, 1, 1))
		})
		It("should support pod anti-affinity with a zone topology", func() {
			affLabels := map[string]string{"security": "s2"}

			// affPods will avoid being scheduled in the same zone
			createPods := func() []*v1.Pod {
				return MakePods(3, test.PodOptions{
					ObjectMeta: metav1.ObjectMeta{Labels: affLabels},
					PodAntiRequirements: []v1.PodAffinityTerm{{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: affLabels,
						},
						TopologyKey: v1.LabelTopologyZone,
					}}})
			}

			top := &v1.TopologySpreadConstraint{TopologyKey: v1.LabelTopologyZone}

			// One of the downsides of late committal is that absent other constraints, it takes multiple batches of
			// scheduling for zonal anti-affinities to work themselves out.  The first schedule, we know that the pod
			// will land in test-zone-1, test-zone-2, or test-zone-3, but don't know which it collapses to until the
			// node is actually created.

			// one pod pod will schedule
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, createPods()...)
			ExpectSkew(ctx, env.Client, "default", top).To(ConsistOf(1))
			// delete all of the unscheduled ones as provisioning will only bind pods passed into the provisioning call
			// the scheduler looks at all pods though, so it may assume a pod from this batch schedules and no others do
			ExpectDeleteAllUnscheduledPods(ctx, env.Client)

			// second pod in a second zone
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, createPods()...)
			ExpectSkew(ctx, env.Client, "default", top).To(ConsistOf(1, 1))
			ExpectDeleteAllUnscheduledPods(ctx, env.Client)

			// third pod in the last zone
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, createPods()...)
			ExpectSkew(ctx, env.Client, "default", top).To(ConsistOf(1, 1, 1))
			ExpectDeleteAllUnscheduledPods(ctx, env.Client)

			// and nothing else can schedule
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, createPods()...)
			ExpectSkew(ctx, env.Client, "default", top).To(ConsistOf(1, 1, 1))
			ExpectDeleteAllUnscheduledPods(ctx, env.Client)
		})
		It("should not schedule pods with affinity to a non-existent pod", func() {
			affLabels := map[string]string{"security": "s2"}
			affPods := MakePods(10, test.PodOptions{
				PodRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelTopologyZone,
				}}})

			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, affPods...)
			// the pod we have affinity to is not in the cluster, so all of these pods are unschedulable
			for _, p := range pods {
				ExpectNotScheduled(ctx, env.Client, p)
			}
		})
		It("should support pod affinity with zone topology (unconstrained target)", func() {
			affLabels := map[string]string{"security": "s2"}

			// the pod that the others have an affinity to
			targetPod := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels}})

			// affPods all want to schedule in the same zone as targetPod, but can't as it's zone is undetermined
			affPods := MakePods(10, test.PodOptions{
				PodRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelTopologyZone,
				}}})

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, append(affPods, targetPod)...)
			top := &v1.TopologySpreadConstraint{TopologyKey: v1.LabelTopologyZone}
			// these pods can't schedule as the pod they have affinity to isn't limited to any particular zone
			for i := range affPods {
				ExpectNotScheduled(ctx, env.Client, affPods[i])
				affPods[i] = ExpectPodExists(ctx, env.Client, affPods[i].Name, affPods[i].Namespace)
			}
			ExpectSkew(ctx, env.Client, "default", top).To(ConsistOf(1))

			// now that targetPod has been scheduled to a node, it's zone is committed and the pods with affinity to it
			// should schedule in the same zone
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, affPods...)
			for _, pod := range affPods {
				ExpectScheduled(ctx, env.Client, pod)
			}
			ExpectSkew(ctx, env.Client, "default", top).To(ConsistOf(11))
		})
		It("should support pod affinity with zone topology (constrained target)", func() {
			affLabels := map[string]string{"security": "s2"}

			// the pod that the others have an affinity to
			affPod1 := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels},
				NodeRequirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelTopologyZone,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"test-zone-1"},
					},
				}})

			// affPods will all be scheduled in the same zone as affPod1
			affPods := MakePods(10, test.PodOptions{
				PodRequirements: []v1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: affLabels,
					},
					TopologyKey: v1.LabelTopologyZone,
				}}})

			affPods = append(affPods, affPod1)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, affPods...)
			top := &v1.TopologySpreadConstraint{TopologyKey: v1.LabelTopologyZone}
			ExpectSkew(ctx, env.Client, "default", top).To(ConsistOf(11))
		})
		It("should handle multiple dependent affinities", func() {
			dbLabels := map[string]string{"type": "db", "spread": "spread"}
			webLabels := map[string]string{"type": "web", "spread": "spread"}
			cacheLabels := map[string]string{"type": "cache", "spread": "spread"}
			uiLabels := map[string]string{"type": "ui", "spread": "spread"}
			for i := 0; i < 50; i++ {
				ExpectApplied(ctx, env.Client, provisioner.DeepCopy())
				// we have to schedule DB -> Web -> Cache -> UI in that order or else there are pod affinity violations
				pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
					test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: dbLabels}}),
					test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: webLabels},
						PodRequirements: []v1.PodAffinityTerm{{
							LabelSelector: &metav1.LabelSelector{MatchLabels: dbLabels},
							TopologyKey:   v1.LabelHostname},
						}}),
					test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: cacheLabels},
						PodRequirements: []v1.PodAffinityTerm{{
							LabelSelector: &metav1.LabelSelector{MatchLabels: webLabels},
							TopologyKey:   v1.LabelHostname},
						}}),
					test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: uiLabels},
						PodRequirements: []v1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{MatchLabels: cacheLabels},
								TopologyKey:   v1.LabelHostname},
						}}),
				)
				for i := range pods {
					ExpectScheduled(ctx, env.Client, pods[i])
				}
				ExpectCleanedUp(ctx, env.Client)
				cluster.Reset(ctx)
			}
		})
		It("should fail to schedule pods with unsatisfiable dependencies", func() {
			dbLabels := map[string]string{"type": "db", "spread": "spread"}
			webLabels := map[string]string{"type": "web", "spread": "spread"}
			ExpectApplied(ctx, env.Client, provisioner)
			// this pods wants to schedule with a non-existent pod, this test just ensures that the scheduling loop
			// doesn't infinite loop
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: dbLabels},
					PodRequirements: []v1.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{MatchLabels: webLabels},
							TopologyKey:   v1.LabelHostname,
						},
					}}),
			)
			ExpectNotScheduled(ctx, env.Client, pods[0])
		})
		It("should filter pod affinity topologies by namespace, no matching pods", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			ExpectApplied(ctx, env.Client, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "other-ns-no-match"}})
			affLabels := map[string]string{"security": "s2"}

			affPod1 := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels, Namespace: "other-ns-no-match"}})
			// affPod2 will try to get scheduled with affPod1
			affPod2 := test.UnschedulablePod(test.PodOptions{PodRequirements: []v1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: affLabels,
				},
				TopologyKey: v1.LabelHostname,
			}}})

			var pods []*v1.Pod
			// creates 10 nodes due to topo spread
			pods = append(pods, MakePods(10, test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: topology,
			})...)
			pods = append(pods, affPod1)
			pods = append(pods, affPod2)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)

			// the target pod gets scheduled
			ExpectScheduled(ctx, env.Client, affPod1)
			// but the one with affinity does not since the target pod is not in the same namespace and doesn't
			// match the namespace list or namespace selector
			ExpectNotScheduled(ctx, env.Client, affPod2)
		})
		It("should filter pod affinity topologies by namespace, matching pods namespace list", func() {
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			ExpectApplied(ctx, env.Client, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "other-ns-list"}})
			affLabels := map[string]string{"security": "s2"}

			affPod1 := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels, Namespace: "other-ns-list"}})
			// affPod2 will try to get scheduled with affPod1
			affPod2 := test.UnschedulablePod(test.PodOptions{PodRequirements: []v1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: affLabels,
				},
				Namespaces:  []string{"other-ns-list"},
				TopologyKey: v1.LabelHostname,
			}}})

			var pods []*v1.Pod
			// create 10 nodes
			pods = append(pods, MakePods(10, test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: topology,
			})...)
			// put our target pod on one of them
			pods = append(pods, affPod1)
			// and our pod with affinity should schedule on the same node
			pods = append(pods, affPod2)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			n1 := ExpectScheduled(ctx, env.Client, affPod1)
			n2 := ExpectScheduled(ctx, env.Client, affPod2)
			// should be scheduled on the same node
			Expect(n1.Name).To(Equal(n2.Name))
		})
		It("should filter pod affinity topologies by namespace, empty namespace selector", func() {
			if env.Version.Minor() < 21 {
				Skip("namespace selector is only supported on K8s >= 1.21.x")
			}
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}

			ExpectApplied(ctx, env.Client, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "empty-ns-selector", Labels: map[string]string{"foo": "bar"}}})
			affLabels := map[string]string{"security": "s2"}

			affPod1 := test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: affLabels, Namespace: "empty-ns-selector"}})
			// affPod2 will try to get scheduled with affPod1
			affPod2 := test.UnschedulablePod(test.PodOptions{PodRequirements: []v1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: affLabels,
				},
				// select all pods in all namespaces since the selector is empty
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{}},
				TopologyKey:       v1.LabelHostname,
			}}})

			var pods []*v1.Pod
			// create 10 nodes
			pods = append(pods, MakePods(10, test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: topology,
			})...)
			// put our target pod on one of them
			pods = append(pods, affPod1)
			// and our pod with affinity should schedule on the same node
			pods = append(pods, affPod2)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
			n1 := ExpectScheduled(ctx, env.Client, affPod1)
			n2 := ExpectScheduled(ctx, env.Client, affPod2)
			// should be scheduled on the same node due to the empty namespace selector
			Expect(n1.Name).To(Equal(n2.Name))
		})
		It("should count topology across multiple provisioners", func() {
			ExpectApplied(ctx, env.Client,
				test.Provisioner(test.ProvisionerOptions{
					Requirements: []v1.NodeSelectorRequirement{{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-1"}}},
				}),
				test.Provisioner(test.ProvisionerOptions{
					Requirements: []v1.NodeSelectorRequirement{{Key: v1.LabelTopologyZone, Operator: v1.NodeSelectorOpIn, Values: []string{"test-zone-2", "test-zone-3"}}},
				}),
			)
			labels := map[string]string{"foo": "bar"}
			topology := v1.TopologySpreadConstraint{
				TopologyKey:       v1.LabelTopologyZone,
				MaxSkew:           1,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				WhenUnsatisfiable: v1.DoNotSchedule,
			}
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.Pods(10, test.UnscheduleablePodOptions(test.PodOptions{
				ObjectMeta:                metav1.ObjectMeta{Labels: labels},
				TopologySpreadConstraints: []v1.TopologySpreadConstraint{topology},
			}))...)
			ExpectSkew(ctx, env.Client, "default", &topology).To(ConsistOf(3, 3, 4))
		})
	})
})

func ExpectDeleteAllUnscheduledPods(ctx2 context.Context, c client.Client) {
	var pods v1.PodList
	Expect(c.List(ctx2, &pods)).To(Succeed())
	for i := range pods.Items {
		if pods.Items[i].Spec.NodeName == "" {
			ExpectDeleted(ctx2, c, &pods.Items[i])
		}
	}
}

var _ = Describe("Taints", func() {
	It("should taint nodes with provisioner taints", func() {
		provisioner.Spec.Taints = []v1.Taint{{Key: "test", Value: "bar", Effect: v1.TaintEffectNoSchedule}}
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
			test.PodOptions{Tolerations: []v1.Toleration{{Effect: v1.TaintEffectNoSchedule, Operator: v1.TolerationOpExists}}},
		))[0]
		node := ExpectScheduled(ctx, env.Client, pod)
		Expect(node.Spec.Taints).To(ContainElement(provisioner.Spec.Taints[0]))
	})
	It("should schedule pods that tolerate provisioner constraints", func() {
		provisioner.Spec.Taints = []v1.Taint{{Key: "test-key", Value: "test-value", Effect: v1.TaintEffectNoSchedule}}
		ExpectApplied(ctx, env.Client, provisioner)
		for _, pod := range ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			// Tolerates with OpExists
			test.UnschedulablePod(test.PodOptions{Tolerations: []v1.Toleration{{Key: "test-key", Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule}}}),
			// Tolerates with OpEqual
			test.UnschedulablePod(test.PodOptions{Tolerations: []v1.Toleration{{Key: "test-key", Value: "test-value", Operator: v1.TolerationOpEqual, Effect: v1.TaintEffectNoSchedule}}}),
		) {
			ExpectScheduled(ctx, env.Client, pod)
		}
		ExpectApplied(ctx, env.Client, provisioner)
		for _, pod := range ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			// Missing toleration
			test.UnschedulablePod(),
			// key mismatch with OpExists
			test.UnschedulablePod(test.PodOptions{Tolerations: []v1.Toleration{{Key: "invalid", Operator: v1.TolerationOpExists}}}),
			// value mismatch
			test.UnschedulablePod(test.PodOptions{Tolerations: []v1.Toleration{{Key: "test-key", Operator: v1.TolerationOpEqual, Effect: v1.TaintEffectNoSchedule}}}),
		) {
			ExpectNotScheduled(ctx, env.Client, pod)
		}
	})
	It("should provision nodes with taints and schedule pods if the taint is only a startup taint", func() {
		provisioner.Spec.StartupTaints = []v1.Taint{{Key: "ignore-me", Value: "nothing-to-see-here", Effect: v1.TaintEffectNoSchedule}}

		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())[0]
		ExpectScheduled(ctx, env.Client, pod)
	})
	It("should not generate taints for OpExists", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{Tolerations: []v1.Toleration{{Key: "test-key", Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoExecute}}}),
		)[0]
		node := ExpectScheduled(ctx, env.Client, pod)
		Expect(node.Spec.Taints).To(HaveLen(1)) // Expect no taints generated beyond the default
	})
})

var _ = Describe("Instance Type Compatibility", func() {
	It("should not schedule if requesting more resources than any instance type has", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						v1.ResourceCPU: resource.MustParse("512"),
					}},
			}))
		ExpectNotScheduled(ctx, env.Client, pod[0])
	})
	It("should launch pods with different archs on different instances", func() {
		provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{{
			Key:      v1.LabelArchStable,
			Operator: v1.NodeSelectorOpIn,
			Values:   []string{v1alpha5.ArchitectureArm64, v1alpha5.ArchitectureAmd64},
		}}
		nodeNames := sets.NewString()
		ExpectApplied(ctx, env.Client, provisioner)
		for _, pod := range ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				NodeSelector: map[string]string{v1.LabelArchStable: v1alpha5.ArchitectureAmd64},
			}),
			test.UnschedulablePod(test.PodOptions{
				NodeSelector: map[string]string{v1.LabelArchStable: v1alpha5.ArchitectureArm64},
			})) {
			node := ExpectScheduled(ctx, env.Client, pod)
			nodeNames.Insert(node.Name)
		}
		Expect(nodeNames.Len()).To(Equal(2))
	})
	It("should exclude instance types that are not supported by the pod constraints (node affinity/instance type)", func() {
		provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{{
			Key:      v1.LabelArchStable,
			Operator: v1.NodeSelectorOpIn,
			Values:   []string{v1alpha5.ArchitectureAmd64},
		}}
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				NodeRequirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelInstanceTypeStable,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"arm-instance-type"},
					},
				}}))
		// arm instance type conflicts with the provisioner limitation of AMD only
		ExpectNotScheduled(ctx, env.Client, pod[0])
	})
	It("should exclude instance types that are not supported by the pod constraints (node affinity/operating system)", func() {
		provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{{
			Key:      v1.LabelArchStable,
			Operator: v1.NodeSelectorOpIn,
			Values:   []string{v1alpha5.ArchitectureAmd64},
		}}
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				NodeRequirements: []v1.NodeSelectorRequirement{
					{
						Key:      v1.LabelOSStable,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"ios"},
					},
				}}))
		// there's an instance with an OS of ios, but it has an arm processor so the provider requirements will
		// exclude it
		ExpectNotScheduled(ctx, env.Client, pod[0])
	})
	It("should exclude instance types that are not supported by the provider constraints (arch)", func() {
		provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{{
			Key:      v1.LabelArchStable,
			Operator: v1.NodeSelectorOpIn,
			Values:   []string{v1alpha5.ArchitectureAmd64},
		}}
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{v1.ResourceCPU: resource.MustParse("14")}}}))
		// only the ARM instance has enough CPU, but it's not allowed per the provisioner
		ExpectNotScheduled(ctx, env.Client, pod[0])
	})
	It("should launch pods with different operating systems on different instances", func() {
		provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{{
			Key:      v1.LabelArchStable,
			Operator: v1.NodeSelectorOpIn,
			Values:   []string{v1alpha5.ArchitectureArm64, v1alpha5.ArchitectureAmd64},
		}}
		nodeNames := sets.NewString()
		ExpectApplied(ctx, env.Client, provisioner)
		for _, pod := range ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				NodeSelector: map[string]string{v1.LabelOSStable: string(v1.Linux)},
			}),
			test.UnschedulablePod(test.PodOptions{
				NodeSelector: map[string]string{v1.LabelOSStable: string(v1.Windows)},
			})) {
			node := ExpectScheduled(ctx, env.Client, pod)
			nodeNames.Insert(node.Name)
		}
		Expect(nodeNames.Len()).To(Equal(2))
	})
	It("should launch pods with different instance type node selectors on different instances", func() {
		provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{{
			Key:      v1.LabelArchStable,
			Operator: v1.NodeSelectorOpIn,
			Values:   []string{v1alpha5.ArchitectureArm64, v1alpha5.ArchitectureAmd64},
		}}
		nodeNames := sets.NewString()
		ExpectApplied(ctx, env.Client, provisioner)
		for _, pod := range ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				NodeSelector: map[string]string{v1.LabelInstanceType: "small-instance-type"},
			}),
			test.UnschedulablePod(test.PodOptions{
				NodeSelector: map[string]string{v1.LabelInstanceTypeStable: "default-instance-type"},
			})) {
			node := ExpectScheduled(ctx, env.Client, pod)
			nodeNames.Insert(node.Name)
		}
		Expect(nodeNames.Len()).To(Equal(2))
	})
	It("should launch pods with different zone selectors on different instances", func() {
		provisioner.Spec.Requirements = []v1.NodeSelectorRequirement{{
			Key:      v1.LabelArchStable,
			Operator: v1.NodeSelectorOpIn,
			Values:   []string{v1alpha5.ArchitectureArm64, v1alpha5.ArchitectureAmd64},
		}}
		nodeNames := sets.NewString()
		ExpectApplied(ctx, env.Client, provisioner)
		for _, pod := range ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				NodeSelector: map[string]string{v1.LabelTopologyZone: "test-zone-1"},
			}),
			test.UnschedulablePod(test.PodOptions{
				NodeSelector: map[string]string{v1.LabelTopologyZone: "test-zone-2"},
			})) {
			node := ExpectScheduled(ctx, env.Client, pod)
			nodeNames.Insert(node.Name)
		}
		Expect(nodeNames.Len()).To(Equal(2))
	})
	It("should launch pods with resources that aren't on any single instance type on different instances", func() {
		cloudProv.InstanceTypes = fake.InstanceTypes(5)
		const fakeGPU1 = "karpenter.sh/super-great-gpu"
		const fakeGPU2 = "karpenter.sh/even-better-gpu"
		cloudProv.InstanceTypes[0].Capacity[fakeGPU1] = resource.MustParse("25")
		cloudProv.InstanceTypes[1].Capacity[fakeGPU2] = resource.MustParse("25")

		nodeNames := sets.NewString()
		ExpectApplied(ctx, env.Client, provisioner)
		for _, pod := range ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{
					Limits: v1.ResourceList{fakeGPU1: resource.MustParse("1")},
				},
			}),
			// Should pack onto a different instance since no instance type has both GPUs
			test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{
					Limits: v1.ResourceList{fakeGPU2: resource.MustParse("1")},
				},
			})) {
			node := ExpectScheduled(ctx, env.Client, pod)
			nodeNames.Insert(node.Name)
		}
		Expect(nodeNames.Len()).To(Equal(2))
	})
	It("should fail to schedule a pod with resources requests that aren't on a single instance type", func() {
		cloudProv.InstanceTypes = fake.InstanceTypes(5)
		const fakeGPU1 = "karpenter.sh/super-great-gpu"
		const fakeGPU2 = "karpenter.sh/even-better-gpu"
		cloudProv.InstanceTypes[0].Capacity[fakeGPU1] = resource.MustParse("25")
		cloudProv.InstanceTypes[1].Capacity[fakeGPU2] = resource.MustParse("25")

		ExpectApplied(ctx, env.Client, provisioner)
		pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{
					Limits: v1.ResourceList{
						fakeGPU1: resource.MustParse("1"),
						fakeGPU2: resource.MustParse("1")},
				},
			}))
		ExpectNotScheduled(ctx, env.Client, pods[0])
	})
	Context("Provider Specific Labels", func() {
		It("should filter instance types that match labels", func() {
			cloudProv.InstanceTypes = fake.InstanceTypes(5)
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{fake.LabelInstanceSize: "large"}}),
				test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{fake.LabelInstanceSize: "small"}}),
			)
			node := ExpectScheduled(ctx, env.Client, pods[0])
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelInstanceTypeStable, "fake-it-4"))
			node = ExpectScheduled(ctx, env.Client, pods[1])
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelInstanceTypeStable, "fake-it-0"))
		})
		It("should not schedule with incompatible labels", func() {
			cloudProv.InstanceTypes = fake.InstanceTypes(5)
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{
					fake.LabelInstanceSize:     "large",
					v1.LabelInstanceTypeStable: cloudProv.InstanceTypes[0].Name,
				}}),
				test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{
					fake.LabelInstanceSize:     "small",
					v1.LabelInstanceTypeStable: cloudProv.InstanceTypes[4].Name,
				}}),
			)
			ExpectNotScheduled(ctx, env.Client, pods[0])
			ExpectNotScheduled(ctx, env.Client, pods[1])
		})
		It("should schedule optional labels", func() {
			cloudProv.InstanceTypes = fake.InstanceTypes(5)
			ExpectApplied(ctx, env.Client, provisioner)
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					// Only some instance types have this key
					{Key: fake.ExoticInstanceLabelKey, Operator: v1.NodeSelectorOpExists},
				}}),
			)
			node := ExpectScheduled(ctx, env.Client, pods[0])
			Expect(node.Labels).To(HaveKey(fake.ExoticInstanceLabelKey))
			Expect(node.Labels).To(HaveKeyWithValue(v1.LabelInstanceTypeStable, cloudProv.InstanceTypes[4].Name))
		})
		It("should schedule without optional labels if disallowed", func() {
			cloudProv.InstanceTypes = fake.InstanceTypes(5)
			ExpectApplied(ctx, env.Client, test.Provisioner())
			pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{NodeRequirements: []v1.NodeSelectorRequirement{
					// Only some instance types have this key
					{Key: fake.ExoticInstanceLabelKey, Operator: v1.NodeSelectorOpDoesNotExist},
				}}),
			)
			node := ExpectScheduled(ctx, env.Client, pods[0])
			Expect(node.Labels).ToNot(HaveKey(fake.ExoticInstanceLabelKey))
		})
	})
})

var _ = Describe("Networking constraints", func() {
	Context("HostPort", func() {
		It("shouldn't co-locate pods that use the same HostPort and protocol (default protocol)", func() {
			port := v1.ContainerPort{
				Name:          "test-port",
				HostPort:      80,
				ContainerPort: 1234,
			}
			pod1 := test.UnschedulablePod()
			pod1.Spec.Containers[0].Ports = append(pod1.Spec.Containers[0].Ports, port)
			pod2 := test.UnschedulablePod()
			pod2.Spec.Containers[0].Ports = append(pod2.Spec.Containers[0].Ports, port)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod1, pod2)
			node1 := ExpectScheduled(ctx, env.Client, pod1)
			node2 := ExpectScheduled(ctx, env.Client, pod2)
			Expect(node1.Name).ToNot(Equal(node2.Name))
		})
		It("shouldn't co-locate pods that use the same HostPort and protocol (specific protocol)", func() {
			port := v1.ContainerPort{
				Name:          "test-port",
				HostPort:      80,
				ContainerPort: 1234,
				Protocol:      "UDP",
			}
			pod1 := test.UnschedulablePod()
			pod1.Spec.Containers[0].Ports = append(pod1.Spec.Containers[0].Ports, port)
			pod2 := test.UnschedulablePod()
			pod2.Spec.Containers[0].Ports = append(pod2.Spec.Containers[0].Ports, port)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod1, pod2)
			node1 := ExpectScheduled(ctx, env.Client, pod1)
			node2 := ExpectScheduled(ctx, env.Client, pod2)
			Expect(node1.Name).ToNot(Equal(node2.Name))
		})
		It("shouldn't co-locate pods that use the same HostPort and IP (default (_))", func() {
			port := v1.ContainerPort{
				Name:          "test-port",
				HostPort:      80,
				ContainerPort: 1234,
			}
			pod1 := test.UnschedulablePod()
			pod1.Spec.Containers[0].Ports = append(pod1.Spec.Containers[0].Ports, port)
			port.HostIP = "1.2.3.4" // Defaulted "0.0.0.0" on pod1 should conflict
			pod2 := test.UnschedulablePod()
			pod2.Spec.Containers[0].Ports = append(pod2.Spec.Containers[0].Ports, port)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod1, pod2)
			node1 := ExpectScheduled(ctx, env.Client, pod1)
			node2 := ExpectScheduled(ctx, env.Client, pod2)
			Expect(node1.Name).ToNot(Equal(node2.Name))
		})
		It("shouldn't co-locate pods that use the same HostPort but a different IP, where one ip is 0.0.0.0", func() {
			port := v1.ContainerPort{
				Name:          "test-port",
				HostPort:      80,
				ContainerPort: 1234,
				Protocol:      "TCP",
				HostIP:        "1.2.3.4",
			}
			pod1 := test.UnschedulablePod()
			pod1.Spec.Containers[0].Ports = append(pod1.Spec.Containers[0].Ports, port)
			pod2 := test.UnschedulablePod()
			port.HostIP = "0.0.0.0" // all interfaces
			pod2.Spec.Containers[0].Ports = append(pod2.Spec.Containers[0].Ports, port)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod1, pod2)
			node1 := ExpectScheduled(ctx, env.Client, pod1)
			node2 := ExpectScheduled(ctx, env.Client, pod2)
			Expect(node1.Name).ToNot(Equal(node2.Name))
		})
		It("shouldn't co-locate pods that use the same HostPort but a different IP, where one ip is 0.0.0.0 (existingNodes)", func() {
			port := v1.ContainerPort{
				Name:          "test-port",
				HostPort:      80,
				ContainerPort: 1234,
				Protocol:      "TCP",
				HostIP:        "1.2.3.4",
			}
			pod1 := test.UnschedulablePod()
			pod1.Spec.Containers[0].Ports = append(pod1.Spec.Containers[0].Ports, port)
			pod2 := test.UnschedulablePod()
			port.HostIP = "0.0.0.0" // all interfaces
			pod2.Spec.Containers[0].Ports = append(pod2.Spec.Containers[0].Ports, port)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod1)
			node1 := ExpectScheduled(ctx, env.Client, pod1)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod2)
			node2 := ExpectScheduled(ctx, env.Client, pod2)
			Expect(node1.Name).ToNot(Equal(node2.Name))
		})
		It("should co-locate pods that use the same HostPort but a different protocol", func() {
			port := v1.ContainerPort{
				Name:          "test-port",
				HostPort:      80,
				ContainerPort: 1234,
				Protocol:      "TCP",
			}
			pod1 := test.UnschedulablePod()
			pod1.Spec.Containers[0].Ports = append(pod1.Spec.Containers[0].Ports, port)
			pod2 := test.UnschedulablePod()
			port.Protocol = "UDP"
			pod2.Spec.Containers[0].Ports = append(pod2.Spec.Containers[0].Ports, port)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod1, pod2)
			node1 := ExpectScheduled(ctx, env.Client, pod1)
			node2 := ExpectScheduled(ctx, env.Client, pod2)
			Expect(node1.Name).To(Equal(node2.Name))
		})
		It("should co-locate pods that use the same HostPort but a different IP", func() {
			port := v1.ContainerPort{
				Name:          "test-port",
				HostPort:      80,
				ContainerPort: 1234,
				Protocol:      "TCP",
				HostIP:        "1.2.3.4",
			}
			pod1 := test.UnschedulablePod()
			pod1.Spec.Containers[0].Ports = append(pod1.Spec.Containers[0].Ports, port)
			pod2 := test.UnschedulablePod()
			port.HostIP = "4.5.6.7"
			pod2.Spec.Containers[0].Ports = append(pod2.Spec.Containers[0].Ports, port)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod1, pod2)
			node1 := ExpectScheduled(ctx, env.Client, pod1)
			node2 := ExpectScheduled(ctx, env.Client, pod2)
			Expect(node1.Name).To(Equal(node2.Name))
		})
		It("should co-locate pods that don't use HostPort", func() {
			port := v1.ContainerPort{
				Name:          "test-port",
				ContainerPort: 1234,
				Protocol:      "TCP",
			}
			pod1 := test.UnschedulablePod()
			pod1.Spec.Containers[0].Ports = append(pod1.Spec.Containers[0].Ports, port)
			pod2 := test.UnschedulablePod()
			pod2.Spec.Containers[0].Ports = append(pod2.Spec.Containers[0].Ports, port)

			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod1, pod2)
			node1 := ExpectScheduled(ctx, env.Client, pod1)
			node2 := ExpectScheduled(ctx, env.Client, pod2)
			Expect(node1.Name).To(Equal(node2.Name))
		})
	})
})

var _ = Describe("Binpacking", func() {
	It("should schedule a small pod on the smallest instance", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
			test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("100M"),
				},
			}}))[0]
		node := ExpectScheduled(ctx, env.Client, pod)
		Expect(node.Labels[v1.LabelInstanceTypeStable]).To(Equal("small-instance-type"))
	})
	It("should schedule a small pod on the smallest possible instance type", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
			test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("2000M"),
				},
			}}))[0]
		node := ExpectScheduled(ctx, env.Client, pod)
		Expect(node.Labels[v1.LabelInstanceTypeStable]).To(Equal("small-instance-type"))
	})
	It("should schedule multiple small pods on the smallest possible instance type", func() {
		opts := test.PodOptions{
			Conditions: []v1.PodCondition{{Type: v1.PodScheduled, Reason: v1.PodReasonUnschedulable, Status: v1.ConditionFalse}},
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("10M"),
				},
			}}
		ExpectApplied(ctx, env.Client, provisioner)
		pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.Pods(5, opts)...)
		nodeNames := sets.NewString()
		for _, p := range pods {
			node := ExpectScheduled(ctx, env.Client, p)
			nodeNames.Insert(node.Name)
			Expect(node.Labels[v1.LabelInstanceTypeStable]).To(Equal("small-instance-type"))
		}
		Expect(nodeNames).To(HaveLen(1))
	})
	It("should create new nodes when a node is at capacity", func() {
		opts := test.PodOptions{
			NodeSelector: map[string]string{v1.LabelArchStable: "amd64"},
			Conditions:   []v1.PodCondition{{Type: v1.PodScheduled, Reason: v1.PodReasonUnschedulable, Status: v1.ConditionFalse}},
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("1.8G"),
				},
			}}
		ExpectApplied(ctx, env.Client, provisioner)
		pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.Pods(40, opts)...)
		nodeNames := sets.NewString()
		for _, p := range pods {
			node := ExpectScheduled(ctx, env.Client, p)
			nodeNames.Insert(node.Name)
			Expect(node.Labels[v1.LabelInstanceTypeStable]).To(Equal("default-instance-type"))
		}
		Expect(nodeNames).To(HaveLen(20))
	})
	It("should pack small and large pods together", func() {
		largeOpts := test.PodOptions{
			NodeSelector: map[string]string{v1.LabelArchStable: "amd64"},
			Conditions:   []v1.PodCondition{{Type: v1.PodScheduled, Reason: v1.PodReasonUnschedulable, Status: v1.ConditionFalse}},
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("1.8G"),
				},
			}}
		smallOpts := test.PodOptions{
			NodeSelector: map[string]string{v1.LabelArchStable: "amd64"},
			Conditions:   []v1.PodCondition{{Type: v1.PodScheduled, Reason: v1.PodReasonUnschedulable, Status: v1.ConditionFalse}},
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("400M"),
				},
			}}

		// Two large pods are all that will fit on the default-instance type (the largest instance type) which will create
		// twenty nodes. This leaves just enough room on each of those newNodes for one additional small pod per node, so we
		// should only end up with 20 newNodes total.
		provPods := append(test.Pods(40, largeOpts), test.Pods(20, smallOpts)...)
		ExpectApplied(ctx, env.Client, provisioner)
		pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, provPods...)
		nodeNames := sets.NewString()
		for _, p := range pods {
			node := ExpectScheduled(ctx, env.Client, p)
			nodeNames.Insert(node.Name)
			Expect(node.Labels[v1.LabelInstanceTypeStable]).To(Equal("default-instance-type"))
		}
		Expect(nodeNames).To(HaveLen(20))
	})
	It("should pack newNodes tightly", func() {
		cloudProv.InstanceTypes = fake.InstanceTypes(5)
		var nodes []*v1.Node
		ExpectApplied(ctx, env.Client, provisioner)
		for _, pod := range ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("4.5")},
				},
			}),
			test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
				},
			})) {
			node := ExpectScheduled(ctx, env.Client, pod)
			nodes = append(nodes, node)
		}
		Expect(nodes).To(HaveLen(2))
		// the first pod consumes nearly all CPU of the largest instance type with no room for the second pod, the
		// second pod is much smaller in terms of resources and should get a smaller node
		Expect(nodes[0].Labels[v1.LabelInstanceTypeStable]).ToNot(Equal(nodes[1].Labels[v1.LabelInstanceTypeStable]))
	})
	It("should handle zero-quantity resource requests", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{"foo.com/weird-resources": resource.MustParse("0")},
					Limits:   v1.ResourceList{"foo.com/weird-resources": resource.MustParse("0")},
				},
			}))
		// requesting a resource of quantity zero of a type unsupported by any instance is fine
		ExpectScheduled(ctx, env.Client, pod[0])
	})
	It("should not schedule pods that exceed every instance type's capacity", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
			test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("2Ti"),
				},
			}}))[0]
		ExpectNotScheduled(ctx, env.Client, pod)
	})
	It("should create new newNodes when a node is at capacity due to pod limits per node", func() {
		opts := test.PodOptions{
			NodeSelector: map[string]string{v1.LabelArchStable: "amd64"},
			Conditions:   []v1.PodCondition{{Type: v1.PodScheduled, Reason: v1.PodReasonUnschedulable, Status: v1.ConditionFalse}},
			ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("1m"),
					v1.ResourceCPU:    resource.MustParse("1m"),
				},
			}}
		ExpectApplied(ctx, env.Client, provisioner)
		pods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.Pods(25, opts)...)
		nodeNames := sets.NewString()
		// all of the test instance types support 5 pods each, so we use the 5 instances of the smallest one for our 25 pods
		for _, p := range pods {
			node := ExpectScheduled(ctx, env.Client, p)
			nodeNames.Insert(node.Name)
			Expect(node.Labels[v1.LabelInstanceTypeStable]).To(Equal("small-instance-type"))
		}
		Expect(nodeNames).To(HaveLen(5))
	})
	It("should take into account initContainer resource requests when binpacking", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
			test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("1Gi"),
					v1.ResourceCPU:    resource.MustParse("1"),
				},
			},
				InitImage: "pause",
				InitResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						v1.ResourceMemory: resource.MustParse("1Gi"),
						v1.ResourceCPU:    resource.MustParse("2"),
					},
				},
			}))[0]
		node := ExpectScheduled(ctx, env.Client, pod)
		Expect(node.Labels[v1.LabelInstanceTypeStable]).To(Equal("default-instance-type"))
	})
	It("should not schedule pods when initContainer resource requests are greater than available instance types", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
			test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceMemory: resource.MustParse("1Gi"),
					v1.ResourceCPU:    resource.MustParse("1"),
				},
			},
				InitImage: "pause",
				InitResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						v1.ResourceMemory: resource.MustParse("1Ti"),
						v1.ResourceCPU:    resource.MustParse("2"),
					},
				},
			}))[0]
		ExpectNotScheduled(ctx, env.Client, pod)
	})
	It("should select for valid instance types, regardless of price", func() {
		// capacity sizes and prices don't correlate here, regardless we should filter and see that all three instance types
		// are valid before preferring the cheapest one 'large'
		cloudProv.InstanceTypes = []*cloudprovider.InstanceType{
			fake.NewInstanceType(fake.InstanceTypeOptions{
				Name: "medium",
				Resources: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("2Gi"),
				},
				Offerings: []cloudprovider.Offering{
					{
						CapacityType: v1alpha5.CapacityTypeOnDemand,
						Zone:         "test-zone-1a",
						Price:        3.00,
						Available:    true,
					},
				},
			}),
			fake.NewInstanceType(fake.InstanceTypeOptions{
				Name: "small",
				Resources: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("1Gi"),
				},
				Offerings: []cloudprovider.Offering{
					{
						CapacityType: v1alpha5.CapacityTypeOnDemand,
						Zone:         "test-zone-1a",
						Price:        2.00,
						Available:    true,
					},
				},
			}),
			fake.NewInstanceType(fake.InstanceTypeOptions{
				Name: "large",
				Resources: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("4"),
					v1.ResourceMemory: resource.MustParse("4Gi"),
				},
				Offerings: []cloudprovider.Offering{
					{
						CapacityType: v1alpha5.CapacityTypeOnDemand,
						Zone:         "test-zone-1a",
						Price:        1.00,
						Available:    true,
					},
				},
			}),
		}
		ExpectApplied(ctx, env.Client, provisioner)
		pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(
			test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU:    resource.MustParse("1m"),
					v1.ResourceMemory: resource.MustParse("1Mi"),
				},
			}},
		))
		node := ExpectScheduled(ctx, env.Client, pod[0])
		// large is the cheapest, so we should pick it, but the other two types are also valid options
		Expect(node.Labels[v1.LabelInstanceTypeStable]).To(Equal("large"))
		// all three options should be passed to the cloud provider
		possibleInstanceType := sets.NewString(pscheduling.NewNodeSelectorRequirements(cloudProv.CreateCalls[0].Spec.Requirements...).Get(v1.LabelInstanceTypeStable).Values()...)
		Expect(possibleInstanceType).To(Equal(sets.NewString("small", "medium", "large")))
	})
})

var _ = Describe("In-Flight Nodes", func() {
	It("should not launch a second node if there is an in-flight node that can support the pod", func() {
		opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("10m"),
			},
		}}
		ExpectApplied(ctx, env.Client, provisioner)
		initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
		node1 := ExpectScheduled(ctx, env.Client, initialPod[0])
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

		secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
		node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
		Expect(node1.Name).To(Equal(node2.Name))
	})
	It("should not launch a second node if there is an in-flight node that can support the pod (node selectors)", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("10m"),
			},
		},
			NodeRequirements: []v1.NodeSelectorRequirement{{
				Key:      v1.LabelTopologyZone,
				Operator: v1.NodeSelectorOpIn,
				Values:   []string{"test-zone-2"},
			}}}))
		node1 := ExpectScheduled(ctx, env.Client, initialPod[0])
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

		// the node gets created in test-zone-2
		secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("10m"),
			},
		},
			NodeRequirements: []v1.NodeSelectorRequirement{{
				Key:      v1.LabelTopologyZone,
				Operator: v1.NodeSelectorOpIn,
				Values:   []string{"test-zone-1", "test-zone-2"},
			}}}))
		// test-zone-2 is in the intersection of their node selectors and the node has capacity, so we shouldn't create a new node
		node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))
		Expect(node1.Name).To(Equal(node2.Name))

		// the node gets created in test-zone-2
		thirdPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("10m"),
			},
		},
			NodeRequirements: []v1.NodeSelectorRequirement{{
				Key:      v1.LabelTopologyZone,
				Operator: v1.NodeSelectorOpIn,
				Values:   []string{"test-zone-1", "test-zone-3"},
			}}}))
		// node is in test-zone-2, so this pod needs a new node
		node3 := ExpectScheduled(ctx, env.Client, thirdPod[0])
		Expect(node1.Name).ToNot(Equal(node3.Name))
	})
	It("should launch a second node if a pod won't fit on the existingNodes node", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("1001m"),
			},
		}}
		initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
		node1 := ExpectScheduled(ctx, env.Client, initialPod[0])
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

		// the node will have 2000m CPU, so these two pods can't both fit on it
		opts.ResourceRequirements.Limits[v1.ResourceCPU] = resource.MustParse("1")
		secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
		node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
		Expect(node1.Name).ToNot(Equal(node2.Name))
	})
	It("should launch a second node if a pod isn't compatible with the existingNodes node (node selector)", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("10m"),
			},
		}}
		initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
		node1 := ExpectScheduled(ctx, env.Client, initialPod[0])
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

		secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
			test.UnschedulablePod(test.PodOptions{NodeSelector: map[string]string{v1.LabelArchStable: "arm64"}}))
		node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
		Expect(node1.Name).ToNot(Equal(node2.Name))
	})
	It("should launch a second node if an in-flight node is terminating", func() {
		opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("10m"),
			},
		}}
		ExpectApplied(ctx, env.Client, provisioner)
		initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
		node1 := ExpectScheduled(ctx, env.Client, initialPod[0])
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

		// delete the node
		node1.Finalizers = nil
		ExpectApplied(ctx, env.Client, node1)
		ExpectDeleted(ctx, env.Client, node1)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

		secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
		node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
		Expect(node1.Name).ToNot(Equal(node2.Name))
	})
	Context("Topology", func() {
		It("should balance pods across zones with in-flight newNodes", func() {
			labels := map[string]string{"foo": "bar"}
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelTopologyZone,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1, 2))

			// reconcile our newNodes with the cluster state so they'll show up as in-flight
			var nodeList v1.NodeList
			Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
			for _, node := range nodeList.Items {
				ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKey{Name: node.Name})
			}

			firstRoundNumNodes := len(nodeList.Items)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(3, 3, 3))
			Expect(env.Client.List(ctx, &nodeList)).To(Succeed())

			// shouldn't create any new newNodes as the in-flight ones can support the pods
			Expect(nodeList.Items).To(HaveLen(firstRoundNumNodes))
		})
		It("should balance pods across hostnames with in-flight newNodes", func() {
			labels := map[string]string{"foo": "bar"}
			topology := []v1.TopologySpreadConstraint{{
				TopologyKey:       v1.LabelHostname,
				WhenUnsatisfiable: v1.DoNotSchedule,
				LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
				MaxSkew:           1,
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1, 1, 1))

			// reconcile our newNodes with the cluster state so they'll show up as in-flight
			var nodeList v1.NodeList
			Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
			for _, node := range nodeList.Items {
				ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKey{Name: node.Name})
			}

			ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov,
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
				test.UnschedulablePod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: labels}, TopologySpreadConstraints: topology}),
			)
			// we prefer to launch new newNodes to satisfy the topology spread even though we could technnically schedule against existingNodes
			ExpectSkew(ctx, env.Client, "default", &topology[0]).To(ConsistOf(1, 1, 1, 1, 1, 1, 1, 1, 1))
		})
	})
	Context("Taints", func() {
		It("should assume pod will schedule to a tainted node with no taints", func() {
			opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("8"),
				},
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
			node1 := ExpectScheduled(ctx, env.Client, initialPod[0])

			// delete the pod so that the node is empty
			ExpectDeleted(ctx, env.Client, initialPod[0])
			node1.Spec.Taints = nil
			ExpectApplied(ctx, env.Client, node1)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

			secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())
			node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
			Expect(node1.Name).To(Equal(node2.Name))
		})
		It("should not assume pod will schedule to a tainted node", func() {
			opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("8"),
				},
			}}
			ExpectApplied(ctx, env.Client, provisioner)
			initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
			node1 := ExpectScheduled(ctx, env.Client, initialPod[0])

			// delete the pod so that the node is empty
			ExpectDeleted(ctx, env.Client, initialPod[0])
			// and taint it
			node1.Spec.Taints = append(node1.Spec.Taints, v1.Taint{
				Key:    "foo.com/taint",
				Value:  "tainted",
				Effect: v1.TaintEffectNoSchedule,
			})
			ExpectApplied(ctx, env.Client, node1)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

			secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())
			node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
			Expect(node1.Name).ToNot(Equal(node2.Name))
		})
		It("should assume pod will schedule to a tainted node with a custom startup taint", func() {
			opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("8"),
				},
			}}
			provisioner.Spec.StartupTaints = append(provisioner.Spec.StartupTaints, v1.Taint{
				Key:    "foo.com/taint",
				Value:  "tainted",
				Effect: v1.TaintEffectNoSchedule,
			})
			ExpectApplied(ctx, env.Client, provisioner)
			initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
			node1 := ExpectScheduled(ctx, env.Client, initialPod[0])

			// delete the pod so that the node is empty
			ExpectDeleted(ctx, env.Client, initialPod[0])
			// startup taint + node not ready taint = 2
			Expect(node1.Spec.Taints).To(HaveLen(2))
			Expect(node1.Spec.Taints).To(ContainElement(v1.Taint{
				Key:    "foo.com/taint",
				Value:  "tainted",
				Effect: v1.TaintEffectNoSchedule,
			}))
			ExpectApplied(ctx, env.Client, node1)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

			secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())
			node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
			Expect(node1.Name).To(Equal(node2.Name))
		})
		It("should not assume pod will schedule to a node with startup taints after initialization", func() {
			startupTaint := v1.Taint{Key: "ignore-me", Value: "nothing-to-see-here", Effect: v1.TaintEffectNoSchedule}
			provisioner.Spec.StartupTaints = []v1.Taint{startupTaint}
			ExpectApplied(ctx, env.Client, provisioner)
			initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())
			node1 := ExpectScheduled(ctx, env.Client, initialPod[0])

			// delete the pod so that the node is empty
			ExpectDeleted(ctx, env.Client, initialPod[0])

			// Mark it initialized which only occurs once the startup taint was removed and re-apply only the startup taint.
			// We also need to add resource capacity as after initialization we assume that kubelet has recorded them.
			node1.Labels[v1alpha5.LabelNodeInitialized] = "true"
			node1.Spec.Taints = []v1.Taint{startupTaint}
			node1.Status.Capacity = v1.ResourceList{v1.ResourcePods: resource.MustParse("10")}
			ExpectApplied(ctx, env.Client, node1)

			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

			// we should launch a new node since the startup taint is there, but was gone at some point
			secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())
			node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
			Expect(node1.Name).ToNot(Equal(node2.Name))
		})
		It("should consider a tainted NotReady node as in-flight even if initialized", func() {
			opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{v1.ResourceCPU: resource.MustParse("10m")},
			}}
			ExpectApplied(ctx, env.Client, provisioner)

			// Schedule to New Node
			pod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))[0]
			node1 := ExpectScheduled(ctx, env.Client, pod)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))
			// Mark Initialized
			node1.Labels[v1alpha5.LabelNodeInitialized] = "true"
			node1.Spec.Taints = []v1.Taint{
				{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoSchedule},
				{Key: v1.TaintNodeUnreachable, Effect: v1.TaintEffectNoSchedule},
			}
			ExpectApplied(ctx, env.Client, node1)
			// Schedule to In Flight Node
			pod = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))[0]
			node2 := ExpectScheduled(ctx, env.Client, pod)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node2))

			Expect(node1.Name).To(Equal(node2.Name))
		})
	})
	Context("Daemonsets", func() {
		It("should track daemonset usage separately so we know how many DS resources are remaining to be scheduled", func() {
			ds := test.DaemonSet(
				test.DaemonSetOptions{PodOptions: test.PodOptions{
					ResourceRequirements: v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("1"),
						v1.ResourceMemory: resource.MustParse("1Gi")}},
				}},
			)
			ExpectApplied(ctx, env.Client, provisioner, ds)
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(ds), ds)).To(Succeed())

			opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("8"),
				},
			}}
			initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
			node1 := ExpectScheduled(ctx, env.Client, initialPod[0])

			// create our daemonset pod and manually bind it to the node
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

			// delete the pod so that the node is empty
			ExpectDeleted(ctx, env.Client, initialPod[0])
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

			ExpectApplied(ctx, env.Client, provisioner, dsPod)
			cluster.ForEachNode(func(f *state.Node) bool {
				Expect(f.DaemonSetRequested.Cpu().AsApproximateFloat64()).To(BeNumerically("~", 0))
				// no pods so we have the full 16 CPU
				Expect(f.Available.Cpu().AsApproximateFloat64()).To(BeNumerically("~", 16))
				return true
			})
			ExpectManualBinding(ctx, env.Client, dsPod, node1)
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(dsPod))

			cluster.ForEachNode(func(f *state.Node) bool {
				Expect(f.DaemonSetRequested.Cpu().AsApproximateFloat64()).To(BeNumerically("~", 1))
				// only the DS pod is bound, so available is reduced by one and the DS requested is incremented by one
				Expect(f.Available.Cpu().AsApproximateFloat64()).To(BeNumerically("~", 15))
				return true
			})

			opts = test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("15"),
				},
			}}
			// this pod should schedule on the existingNodes node as the daemonset pod has already bound, meaning that the
			// remaining daemonset resources should be zero leaving 15 CPUs for the pod
			secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
			node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
			Expect(node1.Name).To(Equal(node2.Name))
		})
		It("should handle unexpected daemonset pods binding to the node", func() {
			ds1 := test.DaemonSet(
				test.DaemonSetOptions{PodOptions: test.PodOptions{
					NodeSelector: map[string]string{
						"my-node-label": "value",
					},
					ResourceRequirements: v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("1"),
						v1.ResourceMemory: resource.MustParse("1Gi")}},
				}},
			)
			ds2 := test.DaemonSet(
				test.DaemonSetOptions{PodOptions: test.PodOptions{
					ResourceRequirements: v1.ResourceRequirements{Requests: v1.ResourceList{
						v1.ResourceCPU: resource.MustParse("1m"),
					}}}})
			ExpectApplied(ctx, env.Client, provisioner, ds1, ds2)
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(ds1), ds1)).To(Succeed())

			opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("8"),
				},
			}}
			initialPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
			node1 := ExpectScheduled(ctx, env.Client, initialPod[0])
			// this label appears on the node for some reason that Karpenter can't track
			node1.Labels["my-node-label"] = "value"
			ExpectApplied(ctx, env.Client, node1)

			// create our daemonset pod and manually bind it to the node
			dsPod := test.UnschedulablePod(test.PodOptions{
				NodeSelector: map[string]string{
					"my-node-label": "value",
				},
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						v1.ResourceCPU:    resource.MustParse("1"),
						v1.ResourceMemory: resource.MustParse("2Gi"),
					}},
			})
			dsPod.OwnerReferences = append(dsPod.OwnerReferences, metav1.OwnerReference{
				APIVersion:         "apps/v1",
				Kind:               "DaemonSet",
				Name:               ds1.Name,
				UID:                ds1.UID,
				Controller:         ptr.Bool(true),
				BlockOwnerDeletion: ptr.Bool(true),
			})

			// delete the pod so that the node is empty
			ExpectDeleted(ctx, env.Client, initialPod[0])
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))

			ExpectApplied(ctx, env.Client, provisioner, dsPod)
			cluster.ForEachNode(func(f *state.Node) bool {
				Expect(f.DaemonSetRequested.Cpu().AsApproximateFloat64()).To(BeNumerically("~", 0))
				// no pods so we have the full 16 CPU
				Expect(f.Available.Cpu().AsApproximateFloat64()).To(BeNumerically("~", 16))
				return true
			})
			ExpectManualBinding(ctx, env.Client, dsPod, node1)
			ExpectReconcileSucceeded(ctx, podStateController, client.ObjectKeyFromObject(dsPod))

			cluster.ForEachNode(func(f *state.Node) bool {
				Expect(f.DaemonSetRequested.Cpu().AsApproximateFloat64()).To(BeNumerically("~", 1))
				// only the DS pod is bound, so available is reduced by one and the DS requested is incremented by one
				Expect(f.Available.Cpu().AsApproximateFloat64()).To(BeNumerically("~", 15))
				return true
			})

			opts = test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{
					v1.ResourceCPU: resource.MustParse("15.5"),
				},
			}}
			// This pod should not schedule on the inflight node as it requires more CPU than we have.  This verifies
			// we don't reintroduce a bug where more daemonsets scheduled than anticipated due to unexepected labels
			// appearing on the node which caused us to compute a negative amount of resources remaining for daemonsets
			// which in turn caused us to mis-calculate the amount of resources that were free on the node.
			secondPod := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod(opts))
			node2 := ExpectScheduled(ctx, env.Client, secondPod[0])
			// must create a new node
			Expect(node1.Name).ToNot(Equal(node2.Name))
		})

	})
	// nolint:gosec
	It("should pack in-flight newNodes before launching new newNodes", func() {
		cloudProv.InstanceTypes = []*cloudprovider.InstanceType{
			fake.NewInstanceType(fake.InstanceTypeOptions{
				Name: "medium",
				Resources: v1.ResourceList{
					// enough CPU for four pods + a bit of overhead
					v1.ResourceCPU:  resource.MustParse("4.25"),
					v1.ResourcePods: resource.MustParse("4"),
				},
			}),
		}
		opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("1"),
			},
		}}

		ExpectApplied(ctx, env.Client, provisioner)

		// scheduling in multiple batches random sets of pods
		for i := 0; i < 10; i++ {
			initialPods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, MakePods(rand.Intn(10), opts)...)
			for _, pod := range initialPods {
				node := ExpectScheduled(ctx, env.Client, pod)
				ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
			}
		}

		// due to the in-flight node support, we should pack existing newNodes before launching new node. The end result
		// is that we should only have some spare capacity on our final node
		nodesWithCPUFree := 0
		cluster.ForEachNode(func(n *state.Node) bool {
			if n.Available.Cpu().AsApproximateFloat64() >= 1 {
				nodesWithCPUFree++
			}
			return true
		})
		Expect(nodesWithCPUFree).To(BeNumerically("<=", 1))
	})
	It("should not launch a second node if there is an in-flight node that can support the pod (#2011)", func() {
		opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("10m"),
			},
		}}

		// there was a bug in cluster state where we failed to identify the instance type resources when using a
		// ProviderRef so modify our provisioner to use the ProviderRef and ensure that the second pod schedules
		// to the existingNodes node
		provisioner.Spec.Provider = nil
		provisioner.Spec.ProviderRef = &v1alpha5.ProviderRef{}

		ExpectApplied(ctx, env.Client, provisioner)
		pod := test.UnschedulablePod(opts)
		ExpectProvisionedNoBinding(ctx, env.Client, provisioningController, prov, pod)
		var nodes v1.NodeList
		Expect(env.Client.List(ctx, &nodes)).To(Succeed())
		Expect(nodes.Items).To(HaveLen(1))
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(&nodes.Items[0]))

		pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodScheduled, Reason: v1.PodReasonUnschedulable, Status: v1.ConditionFalse}}
		ExpectApplied(ctx, env.Client, pod)
		ExpectProvisionedNoBinding(ctx, env.Client, provisioningController, prov, pod)
		Expect(env.Client.List(ctx, &nodes)).To(Succeed())
		// shouldn't create a second node
		Expect(nodes.Items).To(HaveLen(1))
	})
})

var _ = Describe("No Pre-Binding", func() {
	It("should not bind pods to newNodes", func() {
		opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU: resource.MustParse("10m"),
			},
		}}

		var nodeList v1.NodeList
		// shouldn't have any newNodes
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		Expect(nodeList.Items).To(HaveLen(0))

		ExpectApplied(ctx, env.Client, provisioner)
		initialPod := ExpectProvisionedNoBinding(ctx, env.Client, provisioningController, prov, test.UnschedulablePod(opts))
		ExpectNotScheduled(ctx, env.Client, initialPod[0])

		// should launch a single node
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		Expect(nodeList.Items).To(HaveLen(1))
		node1 := &nodeList.Items[0]

		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))
		secondPod := ExpectProvisionedNoBinding(ctx, env.Client, provisioningController, prov, test.UnschedulablePod(opts))
		ExpectNotScheduled(ctx, env.Client, secondPod[0])
		// shouldn't create a second node as it can bind to the existingNodes node
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		Expect(nodeList.Items).To(HaveLen(1))
	})
	It("should handle resource zeroing of extended resources by kubelet", func() {
		// Issue #1459
		opts := test.PodOptions{ResourceRequirements: v1.ResourceRequirements{
			Limits: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU:          resource.MustParse("10m"),
				fake.ResourceGPUVendorA: resource.MustParse("1"),
			},
		}}

		var nodeList v1.NodeList
		// shouldn't have any newNodes
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		Expect(nodeList.Items).To(HaveLen(0))

		ExpectApplied(ctx, env.Client, provisioner)
		initialPod := ExpectProvisionedNoBinding(ctx, env.Client, provisioningController, prov, test.UnschedulablePod(opts))
		ExpectNotScheduled(ctx, env.Client, initialPod[0])

		// should launch a single node
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		Expect(nodeList.Items).To(HaveLen(1))
		node1 := &nodeList.Items[0]

		// simulate kubelet zeroing out the extended resources on the node at startup
		node1.Status.Capacity = map[v1.ResourceName]resource.Quantity{
			fake.ResourceGPUVendorA: resource.MustParse("0"),
		}
		node1.Status.Allocatable = map[v1.ResourceName]resource.Quantity{
			fake.ResourceGPUVendorB: resource.MustParse("0"),
		}

		ExpectApplied(ctx, env.Client, node1)

		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node1))
		secondPod := ExpectProvisionedNoBinding(ctx, env.Client, provisioningController, prov, test.UnschedulablePod(opts))
		ExpectNotScheduled(ctx, env.Client, secondPod[0])
		// shouldn't create a second node as it can bind to the existingNodes node
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		Expect(nodeList.Items).To(HaveLen(1))
	})
	It("should respect self pod affinity without pod binding (zone)", func() {
		// Issue #1975
		affLabels := map[string]string{"security": "s2"}

		pods := MakePods(2, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels: affLabels,
			},
			PodRequirements: []v1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: affLabels,
				},
				TopologyKey: v1.LabelTopologyZone,
			}},
		})
		ExpectApplied(ctx, env.Client, provisioner)
		ExpectProvisionedNoBinding(ctx, env.Client, provisioningController, prov, pods[0])
		var nodeList v1.NodeList
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		for i := range nodeList.Items {
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(&nodeList.Items[i]))
		}
		// the second pod can schedule against the in-flight node, but for that to work we need to be careful
		// in how we fulfill the self-affinity by taking the existing node's domain as a preference over any
		// random viable domain
		ExpectProvisionedNoBinding(ctx, env.Client, provisioningController, prov, pods[1])
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		Expect(nodeList.Items).To(HaveLen(1))
	})
})

var _ = Describe("Volumes", func() {
	It("should launch multiple newNodes if required due to volume limits", func() {
		const csiProvider = "fake.csi.provider"
		cloudProv.InstanceTypes = []*cloudprovider.InstanceType{
			fake.NewInstanceType(
				fake.InstanceTypeOptions{
					Name: "instance-type",
					Resources: map[v1.ResourceName]resource.Quantity{
						v1.ResourceCPU:  resource.MustParse("1024"),
						v1.ResourcePods: resource.MustParse("1024"),
					},
				}),
		}

		provisioner.Spec.Limits = nil
		ExpectApplied(ctx, env.Client, provisioner)
		initialPods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())
		node := ExpectScheduled(ctx, env.Client, initialPods[0])
		csiNode := &storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{
				Name: node.Name,
			},
			Spec: storagev1.CSINodeSpec{
				Drivers: []storagev1.CSINodeDriver{
					{
						Name:   csiProvider,
						NodeID: "fake-node-id",
						Allocatable: &storagev1.VolumeNodeResources{
							Count: ptr.Int32(10),
						},
					},
				},
			},
		}
		ExpectApplied(ctx, env.Client, csiNode)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))

		sc := test.StorageClass(test.StorageClassOptions{
			ObjectMeta:  metav1.ObjectMeta{Name: "my-storage-class"},
			Provisioner: ptr.String(csiProvider),
			Zones:       []string{"test-zone-1"}})
		ExpectApplied(ctx, env.Client, sc)

		var pods []*v1.Pod
		for i := 0; i < 6; i++ {
			pvcA := test.PersistentVolumeClaim(test.PersistentVolumeClaimOptions{
				StorageClassName: ptr.String("my-storage-class"),
				ObjectMeta:       metav1.ObjectMeta{Name: fmt.Sprintf("my-claim-a-%d", i)},
			})
			pvcB := test.PersistentVolumeClaim(test.PersistentVolumeClaimOptions{
				StorageClassName: ptr.String("my-storage-class"),
				ObjectMeta:       metav1.ObjectMeta{Name: fmt.Sprintf("my-claim-b-%d", i)},
			})
			ExpectApplied(ctx, env.Client, pvcA, pvcB)
			pods = append(pods, test.UnschedulablePod(test.PodOptions{
				PersistentVolumeClaims: []string{pvcA.Name, pvcB.Name},
			}))
		}
		ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
		var nodeList v1.NodeList
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		// we need to create a new node as the in-flight one can only contain 5 pods due to the CSINode volume limit
		Expect(nodeList.Items).To(HaveLen(2))
	})
	It("should launch a single node if all pods use the same PVC", func() {
		const csiProvider = "fake.csi.provider"
		cloudProv.InstanceTypes = []*cloudprovider.InstanceType{
			fake.NewInstanceType(
				fake.InstanceTypeOptions{
					Name: "instance-type",
					Resources: map[v1.ResourceName]resource.Quantity{
						v1.ResourceCPU:  resource.MustParse("1024"),
						v1.ResourcePods: resource.MustParse("1024"),
					},
				}),
		}

		provisioner.Spec.Limits = nil
		ExpectApplied(ctx, env.Client, provisioner)
		initialPods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())
		node := ExpectScheduled(ctx, env.Client, initialPods[0])
		csiNode := &storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{
				Name: node.Name,
			},
			Spec: storagev1.CSINodeSpec{
				Drivers: []storagev1.CSINodeDriver{
					{
						Name:   csiProvider,
						NodeID: "fake-node-id",
						Allocatable: &storagev1.VolumeNodeResources{
							Count: ptr.Int32(10),
						},
					},
				},
			},
		}
		ExpectApplied(ctx, env.Client, csiNode)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))

		sc := test.StorageClass(test.StorageClassOptions{
			ObjectMeta:  metav1.ObjectMeta{Name: "my-storage-class"},
			Provisioner: ptr.String(csiProvider),
			Zones:       []string{"test-zone-1"}})
		ExpectApplied(ctx, env.Client, sc)

		pv := test.PersistentVolume(test.PersistentVolumeOptions{
			ObjectMeta: metav1.ObjectMeta{Name: "my-volume"},
			Zones:      []string{"test-zone-1"}})

		pvc := test.PersistentVolumeClaim(test.PersistentVolumeClaimOptions{
			ObjectMeta:       metav1.ObjectMeta{Name: "my-claim"},
			StorageClassName: ptr.String("my-storage-class"),
			VolumeName:       pv.Name,
		})
		ExpectApplied(ctx, env.Client, pv, pvc)

		var pods []*v1.Pod
		for i := 0; i < 100; i++ {
			pods = append(pods, test.UnschedulablePod(test.PodOptions{
				PersistentVolumeClaims: []string{pvc.Name, pvc.Name},
			}))
		}
		ExpectApplied(ctx, env.Client, provisioner)
		ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)
		var nodeList v1.NodeList
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		// 100 of the same PVC should all be schedulable on the same node
		Expect(nodeList.Items).To(HaveLen(1))
	})
	It("should not fail for non-dynamic PVCs", func() {
		const csiProvider = "fake.csi.provider"
		cloudProv.InstanceTypes = []*cloudprovider.InstanceType{
			fake.NewInstanceType(
				fake.InstanceTypeOptions{
					Name: "instance-type",
					Resources: map[v1.ResourceName]resource.Quantity{
						v1.ResourceCPU:  resource.MustParse("1024"),
						v1.ResourcePods: resource.MustParse("1024"),
					},
				}),
		}

		provisioner.Spec.Limits = nil
		ExpectApplied(ctx, env.Client, provisioner)
		initialPods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())
		node := ExpectScheduled(ctx, env.Client, initialPods[0])
		csiNode := &storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{
				Name: node.Name,
			},
			Spec: storagev1.CSINodeSpec{
				Drivers: []storagev1.CSINodeDriver{
					{
						Name:   csiProvider,
						NodeID: "fake-node-id",
						Allocatable: &storagev1.VolumeNodeResources{
							Count: ptr.Int32(10),
						},
					},
				},
			},
		}
		ExpectApplied(ctx, env.Client, csiNode)
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))

		sc := test.StorageClass(test.StorageClassOptions{
			ObjectMeta:  metav1.ObjectMeta{Name: "my-storage-class"},
			Provisioner: ptr.String(csiProvider),
			Zones:       []string{"test-zone-1"}})
		ExpectApplied(ctx, env.Client, sc)

		pv := test.PersistentVolume(test.PersistentVolumeOptions{
			ObjectMeta: metav1.ObjectMeta{Name: "my-volume"},
			Driver:     csiProvider,
			Zones:      []string{"test-zone-1"}})

		pvc := test.PersistentVolumeClaim(test.PersistentVolumeClaimOptions{
			ObjectMeta:       metav1.ObjectMeta{Name: "my-claim"},
			VolumeName:       pv.Name,
			StorageClassName: ptr.String(""),
		})
		ExpectApplied(ctx, env.Client, pv, pvc)

		var pods []*v1.Pod
		for i := 0; i < 5; i++ {
			pods = append(pods, test.UnschedulablePod(test.PodOptions{
				PersistentVolumeClaims: []string{pvc.Name, pvc.Name},
			}))
		}
		ExpectApplied(ctx, env.Client, provisioner)
		_ = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)

		var nodeList v1.NodeList
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		// 5 of the same PVC should all be schedulable on the same node
		Expect(nodeList.Items).To(HaveLen(1))
	})
	It("should not fail for NFS volumes", func() {
		cloudProv.InstanceTypes = []*cloudprovider.InstanceType{
			fake.NewInstanceType(
				fake.InstanceTypeOptions{
					Name: "instance-type",
					Resources: map[v1.ResourceName]resource.Quantity{
						v1.ResourceCPU:  resource.MustParse("1024"),
						v1.ResourcePods: resource.MustParse("1024"),
					},
				}),
		}

		provisioner.Spec.Limits = nil
		ExpectApplied(ctx, env.Client, provisioner)
		initialPods := ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, test.UnschedulablePod())
		node := ExpectScheduled(ctx, env.Client, initialPods[0])
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))

		pv := test.PersistentVolume(test.PersistentVolumeOptions{
			ObjectMeta:       metav1.ObjectMeta{Name: "my-volume"},
			StorageClassName: "nfs",
			Zones:            []string{"test-zone-1"}})
		pv.Spec.NFS = &v1.NFSVolumeSource{
			Server: "fake.server",
			Path:   "/some/path",
		}
		pv.Spec.CSI = nil

		pvc := test.PersistentVolumeClaim(test.PersistentVolumeClaimOptions{
			ObjectMeta:       metav1.ObjectMeta{Name: "my-claim"},
			VolumeName:       pv.Name,
			StorageClassName: ptr.String(""),
		})
		ExpectApplied(ctx, env.Client, pv, pvc)

		var pods []*v1.Pod
		for i := 0; i < 5; i++ {
			pods = append(pods, test.UnschedulablePod(test.PodOptions{
				PersistentVolumeClaims: []string{pvc.Name, pvc.Name},
			}))
		}
		ExpectApplied(ctx, env.Client, provisioner)
		_ = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pods...)

		var nodeList v1.NodeList
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		// 5 of the same PVC should all be schedulable on the same node
		Expect(nodeList.Items).To(HaveLen(1))
	})
	It("should not launch nodes for pods with ephemeral volume using a non-existent storage classes", func() {
		ExpectApplied(ctx, env.Client, provisioner)
		pod := test.UnschedulablePod(test.PodOptions{})
		pod.Spec.Volumes = append(pod.Spec.Volumes, v1.Volume{
			Name: "tmp-ephemeral",
			VolumeSource: v1.VolumeSource{
				Ephemeral: &v1.EphemeralVolumeSource{
					VolumeClaimTemplate: &v1.PersistentVolumeClaimTemplate{
						Spec: v1.PersistentVolumeClaimSpec{
							StorageClassName: ptr.String("non-existent"),
							AccessModes: []v1.PersistentVolumeAccessMode{
								v1.ReadWriteOnce,
							},
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceStorage: resource.MustParse("1Gi"),
								},
							},
						},
					},
				},
			},
		})
		ExpectApplied(ctx, env.Client, provisioner)
		_ = ExpectProvisioned(ctx, env.Client, cluster, recorder, provisioningController, prov, pod)

		var nodeList v1.NodeList
		Expect(env.Client.List(ctx, &nodeList)).To(Succeed())
		// no nodes should be created as the storage class doesn't eixst
		Expect(nodeList.Items).To(HaveLen(0))
	})
})

func MakePods(count int, options test.PodOptions) (pods []*v1.Pod) {
	for i := 0; i < count; i++ {
		pods = append(pods, test.UnschedulablePod(options))
	}
	return pods
}

// nolint:gocyclo
func ExpectMaxSkew(ctx context.Context, c client.Client, namespace string, constraint *v1.TopologySpreadConstraint) Assertion {
	nodes := &v1.NodeList{}
	Expect(c.List(ctx, nodes)).To(Succeed())
	pods := &v1.PodList{}
	Expect(c.List(ctx, pods, scheduling.TopologyListOptions(namespace, constraint.LabelSelector))).To(Succeed())
	skew := map[string]int{}

	nodeMap := map[string]*v1.Node{}
	for i, node := range nodes.Items {
		nodeMap[node.Name] = &nodes.Items[i]
	}

	for i, pod := range pods.Items {
		if scheduling.IgnoredForTopology(&pods.Items[i]) {
			continue
		}
		node := nodeMap[pod.Spec.NodeName]
		if pod.Spec.NodeName == node.Name {
			if constraint.TopologyKey == v1.LabelHostname {
				skew[node.Name]++ // Check node name since hostname labels aren't applied
			}
			if constraint.TopologyKey == v1.LabelTopologyZone {
				if key, ok := node.Labels[constraint.TopologyKey]; ok {
					skew[key]++
				}
			}
			if constraint.TopologyKey == v1alpha5.LabelCapacityType {
				if key, ok := node.Labels[constraint.TopologyKey]; ok {
					skew[key]++
				}
			}
		}
	}

	var minCount = math.MaxInt
	var maxCount = math.MinInt
	for _, count := range skew {
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}
	return Expect(maxCount - minCount)
}

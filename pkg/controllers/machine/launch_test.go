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

package machine_test

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/aws/karpenter-core/pkg/test/expectations"
)

var _ = Describe("Launch", func() {
	var provisioner *v1alpha5.Provisioner

	BeforeEach(func() {
		provisioner = test.Provisioner()
	})
	It("should launch an instance when a new Machine is created", func() {
		machine := test.Machine(v1alpha5.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				},
			},
		})
		ExpectApplied(ctx, env.Client, provisioner, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))

		Expect(cloudProvider.CreateCalls).To(HaveLen(1))
		Expect(cloudProvider.CreatedMachines).To(HaveLen(1))
		_, err := cloudProvider.Get(ctx, machine.Name, "")
		Expect(err).ToNot(HaveOccurred())
	})
	It("should get an instance and hydrate the Machine when the Machine is already created", func() {
		machine := test.Machine(v1alpha5.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				},
			},
		})
		ExpectApplied(ctx, env.Client, provisioner, machine)
		cloudProviderMachine := &v1alpha5.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name: machine.Name,
				Labels: map[string]string{
					v1.LabelInstanceTypeStable: "small-instance-type",
					v1.LabelTopologyZone:       "test-zone-1a",
					v1.LabelTopologyRegion:     "test-zone",
					v1alpha5.LabelCapacityType: v1alpha5.CapacityTypeSpot,
				},
			},
			Status: v1alpha5.MachineStatus{
				ProviderID: test.RandomProviderID(),
				Capacity: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("10"),
					v1.ResourceMemory:           resource.MustParse("100Mi"),
					v1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
				},
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:              resource.MustParse("8"),
					v1.ResourceMemory:           resource.MustParse("80Mi"),
					v1.ResourceEphemeralStorage: resource.MustParse("18Gi"),
				},
			},
		}
		cloudProvider.CreatedMachines[machine.Name] = cloudProviderMachine
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))

		machine = ExpectExists(ctx, env.Client, machine)

		Expect(machine.Status.ProviderID).To(Equal(cloudProviderMachine.Status.ProviderID))
		ExpectResources(machine.Status.Capacity, cloudProviderMachine.Status.Capacity)
		ExpectResources(machine.Status.Allocatable, cloudProviderMachine.Status.Allocatable)

		Expect(machine.Labels).To(HaveKeyWithValue(v1.LabelInstanceTypeStable, "small-instance-type"))
		Expect(machine.Labels).To(HaveKeyWithValue(v1.LabelTopologyZone, "test-zone-1a"))
		Expect(machine.Labels).To(HaveKeyWithValue(v1.LabelTopologyRegion, "test-zone"))
		Expect(machine.Labels).To(HaveKeyWithValue(v1alpha5.LabelCapacityType, v1alpha5.CapacityTypeSpot))
	})
	It("should add the MachineCreated status condition after creating the Machine", func() {
		machine := test.Machine(v1alpha5.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				},
			},
		})
		ExpectApplied(ctx, env.Client, provisioner, machine)
		ExpectReconcileSucceeded(ctx, machineController, client.ObjectKeyFromObject(machine))

		machine = ExpectExists(ctx, env.Client, machine)
		Expect(ExpectStatusConditionExists(machine, v1alpha5.MachineCreated).Status).To(Equal(v1.ConditionTrue))
	})
})

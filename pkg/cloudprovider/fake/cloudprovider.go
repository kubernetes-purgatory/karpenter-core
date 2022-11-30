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

package fake

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/aws/karpenter-core/pkg/apis/core"
	"github.com/aws/karpenter-core/pkg/apis/v1alpha1"
	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/cloudprovider"
	"github.com/aws/karpenter-core/pkg/scheduling"
	"github.com/aws/karpenter-core/pkg/test"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ cloudprovider.CloudProvider = (*CloudProvider)(nil)

type CloudProvider struct {
	InstanceTypes []*cloudprovider.InstanceType

	// CreateCalls contains the arguments for every create call that was made since it was cleared
	mu                 sync.Mutex
	CreateCalls        []*v1alpha1.Machine
	AllowedCreateCalls int
}

var _ cloudprovider.CloudProvider = (*CloudProvider)(nil)

func NewCloudProvider() *CloudProvider {
	return &CloudProvider{
		AllowedCreateCalls: math.MaxInt,
	}
}

func (c *CloudProvider) Create(ctx context.Context, machine *v1alpha1.Machine) (*v1.Node, error) {
	c.mu.Lock()
	c.CreateCalls = append(c.CreateCalls, machine)
	if len(c.CreateCalls) > c.AllowedCreateCalls {
		c.mu.Unlock()
		return &v1.Node{}, fmt.Errorf("erroring as number of AllowedCreateCalls has been exceeded")
	}
	c.mu.Unlock()

	requirements := scheduling.NewNodeSelectorRequirements(machine.Spec.Requirements...)
	instanceTypes := lo.Filter(lo.Must(c.GetInstanceTypes(ctx, &v1alpha5.Provisioner{})), func(i *cloudprovider.InstanceType, _ int) bool {
		return requirements.Get(v1.LabelInstanceTypeStable).Has(i.Name)
	})
	// Order instance types so that we get the cheapest instance types of the available offerings
	sort.Slice(instanceTypes, func(i, j int) bool {
		iOfferings := instanceTypes[i].Offerings.Available()
		jOfferings := instanceTypes[j].Offerings.Available()
		return cheapestOfferingPrice(iOfferings, requirements) < cheapestOfferingPrice(jOfferings, requirements)
	})

	name := test.RandomName()
	instanceType := instanceTypes[0]
	// Labels
	labels := map[string]string{}
	for key, requirement := range instanceType.Requirements {
		if requirement.Len() == 1 {
			labels[key] = requirement.Values()[0]
		}
	}
	// Find Offering
	for _, o := range instanceType.Offerings.Available() {
		if requirements.Compatible(scheduling.NewRequirements(
			scheduling.NewRequirement(v1.LabelTopologyZone, v1.NodeSelectorOpIn, o.Zone),
			scheduling.NewRequirement(core.LabelCapacityType, v1.NodeSelectorOpIn, o.CapacityType),
		)) == nil {
			labels[v1.LabelTopologyZone] = o.Zone
			labels[core.LabelCapacityType] = o.CapacityType
			break
		}
	}
	n := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: v1.NodeSpec{
			ProviderID: fmt.Sprintf("fake://%s", name),
		},
	}
	return n, nil
}

func (c *CloudProvider) GetInstanceTypes(_ context.Context, _ *v1alpha5.Provisioner) ([]*cloudprovider.InstanceType, error) {
	if c.InstanceTypes != nil {
		return c.InstanceTypes, nil
	}
	return []*cloudprovider.InstanceType{
		NewInstanceType(InstanceTypeOptions{
			Name: "default-instance-type",
		}),
		NewInstanceType(InstanceTypeOptions{
			Name: "small-instance-type",
			Resources: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU:    resource.MustParse("2"),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
		}),
		NewInstanceType(InstanceTypeOptions{
			Name: "gpu-vendor-instance-type",
			Resources: map[v1.ResourceName]resource.Quantity{
				ResourceGPUVendorA: resource.MustParse("2"),
			}}),
		NewInstanceType(InstanceTypeOptions{
			Name: "gpu-vendor-b-instance-type",
			Resources: map[v1.ResourceName]resource.Quantity{
				ResourceGPUVendorB: resource.MustParse("2"),
			},
		}),
		NewInstanceType(InstanceTypeOptions{
			Name:             "arm-instance-type",
			Architecture:     "arm64",
			OperatingSystems: sets.NewString("ios", string(v1.Linux), string(v1.Windows), "darwin"),
			Resources: map[v1.ResourceName]resource.Quantity{
				v1.ResourceCPU:    resource.MustParse("16"),
				v1.ResourceMemory: resource.MustParse("128Gi"),
			},
		}),
		NewInstanceType(InstanceTypeOptions{
			Name: "single-pod-instance-type",
			Resources: map[v1.ResourceName]resource.Quantity{
				v1.ResourcePods: resource.MustParse("1"),
			},
		}),
	}, nil
}

func (c *CloudProvider) Delete(context.Context, *v1.Node) error {
	return nil
}

// Name returns the CloudProvider implementation name.
func (c *CloudProvider) Name() string {
	return "fake"
}

// cheapestOfferingPrice gets the cheapest price of an offering on an instance type given
// the node requirements
func cheapestOfferingPrice(ofs []cloudprovider.Offering, requirements scheduling.Requirements) float64 {
	minPrice := math.MaxFloat64
	for _, of := range ofs {
		if requirements.Get(core.LabelCapacityType).Has(of.CapacityType) && requirements.Get(v1.LabelTopologyZone).Has(of.Zone) {
			minPrice = math.Min(minPrice, of.Price)
		}
	}
	return minPrice
}

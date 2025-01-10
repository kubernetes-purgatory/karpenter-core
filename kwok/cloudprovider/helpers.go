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

package kwok

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/karpenter/kwok/apis/v1alpha1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

var (
	// AWS uses (family).(size) format
	awsRegexp = regexp.MustCompile(`^\w+\.(nano|micro|small|medium|large|\d*xlarge|metal)$`)

	familyDelim = regexp.MustCompile(`[.-]`)
)

// Wrap cloudprovider.Offerings with NodeSelectorRequirements for post-json processing translation
type KWOKOfferings []KWOKOffering

type KWOKOffering struct {
	cloudprovider.Offering
	Requirements []corev1.NodeSelectorRequirement
}

type InstanceTypeOptions struct {
	Name             string              `json:"name"`
	Offerings        KWOKOfferings       `json:"offerings"`
	Architecture     string              `json:"architecture"`
	OperatingSystems []corev1.OSName     `json:"operatingSystems"`
	Resources        corev1.ResourceList `json:"resources"`

	// These are used for setting default requirements, they should not be used
	// for setting arbitrary node labels.  Set the labels on the created NodePool for
	// that use case.
	instanceTypeLabels map[string]string
}

//go:embed instance_types.json
var rawInstanceTypes []byte

// ConstructInstanceTypes create many instance types based on the embedded instance type data
func ConstructInstanceTypes() ([]*cloudprovider.InstanceType, error) {
	var instanceTypes []*cloudprovider.InstanceType
	var instanceTypeOptions []InstanceTypeOptions

	if err := json.Unmarshal(rawInstanceTypes, &instanceTypeOptions); err != nil {
		return nil, fmt.Errorf("could not parse JSON data: %w", err)
	}

	for _, opts := range instanceTypeOptions {
		opts = setDefaultOptions(opts)
		instanceTypes = append(instanceTypes, newInstanceType(opts))
	}
	return instanceTypes, nil
}

// parseSizeFromType will attempt to discover the instance size if it matches a special AWS format.
// If not, fall back to the cpu value.  This works for both Azure and GCP (and the "generic" instances
// generated by tools/gen_instances.go)
func parseSizeFromType(ty, cpu string) string {
	if matches := awsRegexp.FindStringSubmatch(ty); matches != nil {
		return matches[1]
	}

	return cpu
}

// parseFamilyFromType will attempt to discover the instance family from the node type.  Some examples of
// well-known cloud provider formats:
//
// AWS   - [Family].[Size]
// GCP   - [Family]-[Configuration]-[# of vCPUs])
// Azure - [Family] + [Sub-family]* + [# of vCPUs] + ... + [Version]
//
// So here, we split on [.-], and if that fails, fall back to the first character of the instance type name.
func parseFamilyFromType(instanceType string) string {
	if instanceType == "" {
		return ""
	}

	familySplit := familyDelim.Split(instanceType, 2)
	if len(familySplit) < 2 {
		return instanceType[0:1]
	}
	return familySplit[0]
}

func setDefaultOptions(opts InstanceTypeOptions) InstanceTypeOptions {
	var cpu, memory string
	for res, q := range opts.Resources {
		switch res {
		case corev1.ResourceCPU:
			cpu = q.String()
		case corev1.ResourceMemory:
			memory = q.String()
		}
	}

	opts.instanceTypeLabels = map[string]string{
		v1alpha1.InstanceSizeLabelKey:   parseSizeFromType(opts.Name, cpu),
		v1alpha1.InstanceFamilyLabelKey: parseFamilyFromType(opts.Name),
		v1alpha1.InstanceCPULabelKey:    cpu,
		v1alpha1.InstanceMemoryLabelKey: memory,
	}

	// if the user specified a different pod limit, override the default
	opts.Resources = lo.Assign(corev1.ResourceList{
		corev1.ResourcePods: resource.MustParse("110"), // Default number of pods on a node in Kubernetes
	}, opts.Resources)

	// make sure all the instance types are available
	for i := range opts.Offerings {
		opts.Offerings[i].OfferingAvailabilityResolver = cloudprovider.TrueStaticAvailabilityResolver
	}

	return opts
}

func newInstanceType(options InstanceTypeOptions) *cloudprovider.InstanceType {
	osNames := lo.Map(options.OperatingSystems, func(os corev1.OSName, _ int) string { return string(os) })

	zones := lo.Uniq(lo.Flatten(lo.Map(options.Offerings, func(o KWOKOffering, _ int) []string {
		req, _ := lo.Find(o.Requirements, func(req corev1.NodeSelectorRequirement) bool {
			return req.Key == corev1.LabelTopologyZone
		})
		return req.Values
	})))
	capacityTypes := lo.Uniq(lo.Flatten(lo.Map(options.Offerings, func(o KWOKOffering, _ int) []string {
		req, _ := lo.Find(o.Requirements, func(req corev1.NodeSelectorRequirement) bool {
			return req.Key == v1.CapacityTypeLabelKey
		})
		return req.Values
	})))

	requirements := scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, options.Name),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, options.Architecture),
		scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, osNames...),
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zones...),
		scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityTypes...),
		scheduling.NewRequirement(v1alpha1.InstanceSizeLabelKey, corev1.NodeSelectorOpIn, options.instanceTypeLabels[v1alpha1.InstanceSizeLabelKey]),
		scheduling.NewRequirement(v1alpha1.InstanceFamilyLabelKey, corev1.NodeSelectorOpIn, options.instanceTypeLabels[v1alpha1.InstanceFamilyLabelKey]),
		scheduling.NewRequirement(v1alpha1.InstanceCPULabelKey, corev1.NodeSelectorOpIn, options.instanceTypeLabels[v1alpha1.InstanceCPULabelKey]),
		scheduling.NewRequirement(v1alpha1.InstanceMemoryLabelKey, corev1.NodeSelectorOpIn, options.instanceTypeLabels[v1alpha1.InstanceMemoryLabelKey]),
	)

	return &cloudprovider.InstanceType{
		Name:         options.Name,
		Requirements: requirements,
		Offerings: lo.Map(options.Offerings, func(off KWOKOffering, _ int) cloudprovider.Offering {
			return cloudprovider.Offering{
				OfferingAvailabilityResolver: off.Offering.OfferingAvailabilityResolver,
				Requirements: scheduling.NewRequirements(lo.Map(off.Requirements, func(req corev1.NodeSelectorRequirement, _ int) *scheduling.Requirement {
					return scheduling.NewRequirement(req.Key, req.Operator, req.Values...)
				})...),
				Price:     off.Offering.Price,
			}
		}),
		Capacity: options.Resources,
		Overhead: &cloudprovider.InstanceTypeOverhead{
			KubeReserved: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("10Mi"),
			},
		},
	}
}

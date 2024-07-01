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

package v1

import (
	"context"
	"strings"

	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/karpenter/pkg/apis/v1beta1"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
)

// Convert v1 NodePool to v1beta1 NodePool
func (in *NodePool) ConvertTo(ctx context.Context, to apis.Convertible) error {
	v1beta1NP := to.(*v1beta1.NodePool)
	v1beta1NP.Name = in.Name
	v1beta1NP.UID = in.UID
	v1beta1NP.Labels = in.Labels
	v1beta1NP.Annotations = in.Annotations
	in.Spec.convertTo(ctx, &v1beta1NP.Spec)

	// Convert v1 status
	v1beta1NP.Status.Resources = in.Status.Resources

	v1beta1NP.Annotations = lo.Assign(v1beta1NP.Annotations, map[string]string{
		NodePoolHashVersion:              v1beta1NP.Hash(),
		NodePoolHashVersionAnnotationKey: v1beta1.NodePoolHashVersion,
	})

	return nil
}

func (in *NodePoolSpec) convertTo(ctx context.Context, v1beta1np *v1beta1.NodePoolSpec) {
	v1beta1np.Weight = in.Weight
	v1beta1np.Limits = v1beta1.Limits(in.Limits)
	in.Disruption.convertTo(&v1beta1np.Disruption)
	in.Template.convertTo(ctx, &v1beta1np.Template)
}

func (in *Disruption) convertTo(v1beta1np *v1beta1.Disruption) {
	v1beta1np.ConsolidateAfter = (*v1beta1.NillableDuration)(in.ConsolidateAfter)
	v1beta1np.ConsolidationPolicy = v1beta1.ConsolidationPolicy(in.ConsolidationPolicy)
	v1beta1np.ExpireAfter = v1beta1.NillableDuration(in.ExpireAfter)
	v1beta1np.Budgets = lo.Map(in.Budgets, func(v1Budget Budget, _ int) v1beta1.Budget {
		return v1beta1.Budget{
			Nodes:    v1Budget.Nodes,
			Schedule: v1Budget.Schedule,
			Duration: v1Budget.Duration,
		}
	})
}

func (in *NodeClaimTemplate) convertTo(ctx context.Context, v1beta1np *v1beta1.NodeClaimTemplate) {
	v1beta1np.Annotations = in.Annotations
	v1beta1np.Labels = in.Labels
	v1beta1np.Spec.Taints = in.Spec.Taints
	v1beta1np.Spec.StartupTaints = in.Spec.StartupTaints
	v1beta1np.Spec.Requirements = lo.Map(in.Spec.Requirements, func(v1Requirements NodeSelectorRequirementWithMinValues, _ int) v1beta1.NodeSelectorRequirementWithMinValues {
		return v1beta1.NodeSelectorRequirementWithMinValues{
			NodeSelectorRequirement: v1.NodeSelectorRequirement{
				Key:      v1Requirements.Key,
				Values:   v1Requirements.Values,
				Operator: v1Requirements.Operator,
			},
			MinValues: v1Requirements.MinValues,
		}
	})

	if in.Spec.NodeClassRef != nil {
		nodeclass, found := lo.Find(injection.GetNodeClasses(ctx), func(nc schema.GroupVersionKind) bool {
			return nc.Kind == in.Spec.NodeClassRef.Kind && nc.Group == in.Spec.NodeClassRef.Group
		})
		v1beta1np.Spec.NodeClassRef = &v1beta1.NodeClassReference{
			Kind:       in.Spec.NodeClassRef.Kind,
			Name:       in.Spec.NodeClassRef.Name,
			APIVersion: lo.Ternary(found, nodeclass.GroupVersion().String(), ""),
		}
	}

	// Need to implement Kubelet Conversion
}

// Convert v1beta1 NodePool to V1 NodePool
func (in *NodePool) ConvertFrom(ctx context.Context, v1beta1np apis.Convertible) error {
	v1beta1NP := v1beta1np.(*v1beta1.NodePool)
	in.Name = v1beta1NP.Name
	in.UID = v1beta1NP.UID
	in.Annotations = v1beta1NP.Annotations
	in.Labels = v1beta1NP.Labels

	// Convert v1beta1 status
	in.Status.Resources = v1beta1NP.Status.Resources

	in.Spec.convertFrom(ctx, &v1beta1NP.Spec)
	in.Annotations = lo.Assign(in.Annotations, map[string]string{
		NodePoolHashVersion:              in.Hash(),
		NodePoolHashVersionAnnotationKey: NodePoolHashVersion,
	})
	return nil
}

func (in *NodePoolSpec) convertFrom(ctx context.Context, v1beta1np *v1beta1.NodePoolSpec) {
	in.Weight = v1beta1np.Weight
	in.Limits = Limits(v1beta1np.Limits)
	in.Disruption.convertFrom(&v1beta1np.Disruption)
	in.Template.convertFrom(ctx, &v1beta1np.Template)
}

func (in *Disruption) convertFrom(v1beta1np *v1beta1.Disruption) {
	in.ConsolidateAfter = (*NillableDuration)(v1beta1np.ConsolidateAfter)
	in.ConsolidationPolicy = ConsolidationPolicy(v1beta1np.ConsolidationPolicy)
	in.ExpireAfter = NillableDuration(v1beta1np.ExpireAfter)
	in.Budgets = lo.Map(v1beta1np.Budgets, func(v1beta1Budget v1beta1.Budget, _ int) Budget {
		return Budget{
			Nodes:    v1beta1Budget.Nodes,
			Schedule: v1beta1Budget.Schedule,
			Duration: v1beta1Budget.Duration,
		}
	})
}

func (in *NodeClaimTemplate) convertFrom(ctx context.Context, v1beta1np *v1beta1.NodeClaimTemplate) {
	in.Annotations = v1beta1np.Annotations
	in.Labels = v1beta1np.Labels
	in.Spec.Taints = v1beta1np.Spec.Taints
	in.Spec.StartupTaints = v1beta1np.Spec.StartupTaints
	in.Spec.Requirements = lo.Map(v1beta1np.Spec.Requirements, func(v1beta1Requirements v1beta1.NodeSelectorRequirementWithMinValues, _ int) NodeSelectorRequirementWithMinValues {
		return NodeSelectorRequirementWithMinValues{
			NodeSelectorRequirement: v1.NodeSelectorRequirement{
				Key:      v1beta1Requirements.Key,
				Values:   v1beta1Requirements.Values,
				Operator: v1beta1Requirements.Operator,
			},
			MinValues: v1beta1Requirements.MinValues,
		}
	})

	nodeclasses := injection.GetNodeClasses(ctx)
	if v1beta1np.Spec.NodeClassRef != nil {
		in.Spec.NodeClassRef = &NodeClassReference{
			Name:  v1beta1np.Spec.NodeClassRef.Name,
			Kind:  lo.Ternary(v1beta1np.Spec.NodeClassRef.Kind == "", nodeclasses[0].Kind, v1beta1np.Spec.NodeClassRef.Kind),
			Group: lo.Ternary(v1beta1np.Spec.NodeClassRef.APIVersion == "", nodeclasses[0].Group, strings.Split(v1beta1np.Spec.NodeClassRef.APIVersion, "/")[0]),
		}
	}

	// Need to implement Kubelet Conversion
}

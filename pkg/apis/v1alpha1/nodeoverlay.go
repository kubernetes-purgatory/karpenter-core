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

package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
type NodeOverlayList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeOverlay `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=nodeoverlays,scope=Cluster,categories=karpenter
type NodeOverlay struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NodeOverlaySpec `json:"spec"`
}

type NodeOverlaySpec struct {
	// Selector matches against simulated nodes and modifies their scheduling properties. Matches all if empty.
	// +optional
	Selector []v1.NodeSelectorRequirement `json:"selector,omitempty"`
	// PricePercent modifies the price of the simulated node (PriceAdjustment + (Price * PricePercent / 100)).
	// +optional
	PricePercent *int `json:"pricePercent,omitempty"`

	//
	// The following fields are not yet implemented
	//

	// PriceAdjustment modifies the price of the simulated node (PriceAdjustment + (Price * PricePercent / 100)).
	// +optional
	// PriceAdjustment *int `json:"priceAdjustment,omitempty"`

	// Requirements add additional scheduling requirements to the Node, which may be targeted by pod scheduling constraints
	// This feature is not currently implemented
	// +optional
	// Requirements []v1.NodeSelectorRequirement `json:"requirements,omitempty"`

	// Capacity adds resource capacities to the simulated Node, but will not replace existing values.
	// This feature is not currently implemented
	// +optional
	// Capacity v1.ResourceList

	// Overhead subtracts resources from the simulated Node
	// This feature is not currently implemented
	// +optional
	// Overhead v1.ResourceList `json:"overhead,omitempty"`
}

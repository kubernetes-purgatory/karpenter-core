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

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
)

// NodeClaimStatus defines the observed state of NodeClaim
type NodeClaimStatus struct {
	// NodeName is the name of the corresponding node object
	// +optional
	NodeName string `json:"nodeName,omitempty"`
	// ProviderID of the corresponding node object
	// +optional
	ProviderID string `json:"providerID,omitempty"`
	// Capacity is the estimated full capacity of the node
	// +optional
	Capacity v1.ResourceList `json:"capacity,omitempty"`
	// Allocatable is the estimated allocatable capacity of the node
	// +optional
	Allocatable v1.ResourceList `json:"allocatable,omitempty"`
	// Conditions contains signals for health and readiness
	// +optional
	Conditions apis.Conditions `json:"conditions,omitempty"`
}

func (in *NodeClaim) StatusConditions() apis.ConditionManager {
	return apis.NewLivingConditionSet(
		NodeLaunched,
		NodeRegistered,
		NodeInitialized,
	).Manage(in)
}

var LivingConditions = []apis.ConditionType{
	NodeLaunched,
	NodeRegistered,
	NodeInitialized,
}

var (
	NodeLaunched              apis.ConditionType = "NodeLaunched"
	NodeRegistered            apis.ConditionType = "NodeRegistered"
	NodeInitialized           apis.ConditionType = "NodeInitialized"
	NodeEmpty                 apis.ConditionType = "NodeEmpty"
	NodeDrifted               apis.ConditionType = "NodeDrifted"
	NodeExpired               apis.ConditionType = "NodeExpired"
	NodeUnderutilized         apis.ConditionType = "NodeUnderutilized"
	NodeDeprovisioningBlocked apis.ConditionType = "NodeDeprovisioningBlocked"
)

func (in *NodeClaim) GetConditions() apis.Conditions {
	return in.Status.Conditions
}

func (in *NodeClaim) SetConditions(conditions apis.Conditions) {
	in.Status.Conditions = conditions
}

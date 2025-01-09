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
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/apis"
)

// Well known labels and resources
const (
	ArchitectureAmd64    = "amd64"
	ArchitectureArm64    = "arm64"
	CapacityTypeSpot     = "spot"
	CapacityTypeOnDemand = "on-demand"
)

// Karpenter specific domains and labels
const (
	NodePoolLabelKey        = apis.Group + "/nodepool"
	NodeInitializedLabelKey = apis.Group + "/initialized"
	NodeRegisteredLabelKey  = apis.Group + "/registered"
	CapacityTypeLabelKey    = apis.Group + "/capacity-type"
)

// Karpenter specific annotations
const (
	DoNotDisruptAnnotationKey                  = apis.Group + "/do-not-disrupt"
	ProviderCompatibilityAnnotationKey         = apis.CompatibilityGroup + "/provider"
	NodePoolHashAnnotationKey                  = apis.Group + "/nodepool-hash"
	NodePoolHashVersionAnnotationKey           = apis.Group + "/nodepool-hash-version"
	NodeClaimTerminationTimestampAnnotationKey = apis.Group + "/nodeclaim-termination-timestamp"
)

// Karpenter specific finalizers
const (
	TerminationFinalizer = apis.Group + "/termination"
)

var (
	// RestrictedLabelDomains are reserved by karpenter
	RestrictedLabelDomains = sets.New(
		apis.Group,
	)

	K8sLabelDomains = sets.New(
		"kubernetes.io",
		"k8s.io",
	)

	// WellKnownLabels are labels that belong to the RestrictedLabelDomains or K8sLabelDomains but allowed.
	// Karpenter is aware of these labels, and they can be used to further narrow down
	// the range of the corresponding values by either nodepool or pods.
	WellKnownLabels = sets.New(
		NodePoolLabelKey,
		v1.LabelTopologyZone,
		v1.LabelTopologyRegion,
		v1.LabelInstanceTypeStable,
		v1.LabelArchStable,
		v1.LabelOSStable,
		CapacityTypeLabelKey,
		v1.LabelWindowsBuild,
	)

	// RestrictedLabels are labels that should not be used
	// because they may interfere with the internal provisioning logic.
	RestrictedLabels = sets.New(
		v1.LabelHostname,
	)

	// NormalizedLabels translate aliased concepts into the controller's
	// WellKnownLabels. Pod requirements are translated for compatibility.
	NormalizedLabels = map[string]string{
		v1.LabelFailureDomainBetaZone:   v1.LabelTopologyZone,
		"beta.kubernetes.io/arch":       v1.LabelArchStable,
		"beta.kubernetes.io/os":         v1.LabelOSStable,
		v1.LabelInstanceType:            v1.LabelInstanceTypeStable,
		v1.LabelFailureDomainBetaRegion: v1.LabelTopologyRegion,
	}
)

// IsRestrictedLabel is used for runtime validation of requirements.
// Returns an error if the label is restricted. E.g. using .karpenter.sh suffix.
func IsRestrictedLabel(key string) error {
	if WellKnownLabels.Has(key) {
		return nil
	}

	labelDomain := GetLabelDomain(key)
	for restrictedLabelDomain := range RestrictedLabelDomains {
		if labelDomain == restrictedLabelDomain || strings.HasSuffix(labelDomain, "."+restrictedLabelDomain) {
			return fmt.Errorf("label %s is restricted; specify a well known label: %v, or a custom label that does not use a restricted domain: %v", key, sets.List(WellKnownLabels), sets.List(RestrictedLabelDomains))
		}
	}

	if RestrictedLabels.Has(key) {
		return fmt.Errorf("label %s is restricted; specify a well known label: %v, or a custom label that does not use a restricted domain: %v", key, sets.List(WellKnownLabels), sets.List(RestrictedLabelDomains))
	}

	return nil
}

// IsValidLabelToSync returns true if the label key is allowed to be synced to the Node object centrally by Karpenter.
func IsValidToSyncCentrallyLabel(key string) bool {
	// TODO(enxebre): consider this to be configurable with runtime flag.
	notValidToSyncLabel := WellKnownLabels

	return !notValidToSyncLabel.Has(key)
}

// IsKubeletLabel returns true if the label key is one that kubelets are allowed to set on their own Node object.
// This function is similar the one used by the node restriction admission https://github.com/kubernetes/kubernetes/blob/e319c541f144e9bee6160f1dd8671638a9029f4c/staging/src/k8s.io/kubelet/pkg/apis/well_known_labels.go#L67
// but karpenter also restricts the known labels to be passed to kubelet. Only the kubeletLabelNamespaces are allowed.
func IsKubeletLabel(key string) bool {
	if WellKnownLabels.Has(key) {
		return false
	}

	if !isKubernetesLabel(key) {
		return true
	}

	namespace := GetLabelDomain(key)
	for allowedNamespace := range kubeletLabelNamespaces {
		if namespace == allowedNamespace || strings.HasSuffix(namespace, "."+allowedNamespace) {
			return true
		}
	}

	return false
}

func isKubernetesLabel(key string) bool {
	namespace := GetLabelDomain(key)
	if namespace == "kubernetes.io" || strings.HasSuffix(namespace, ".kubernetes.io") {
		return true
	}
	if namespace == "k8s.io" || strings.HasSuffix(namespace, ".k8s.io") {
		return true
	}
	return false
}

var kubeletLabelNamespaces = sets.NewString(
	v1.LabelNamespaceSuffixKubelet,
	v1.LabelNamespaceSuffixNode,
)

func GetLabelDomain(key string) string {
	if parts := strings.SplitN(key, "/", 2); len(parts) == 2 {
		return parts[0]
	}
	return ""
}

func NodeClassLabelKey(gk schema.GroupKind) string {
	return fmt.Sprintf("%s/%s", gk.Group, strings.ToLower(gk.Kind))
}

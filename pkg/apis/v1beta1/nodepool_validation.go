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
	"context"
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"knative.dev/pkg/apis"
)

func (in *NodePool) SupportedVerbs() []admissionregistrationv1.OperationType {
	return []admissionregistrationv1.OperationType{
		admissionregistrationv1.Create,
		admissionregistrationv1.Update,
	}
}

func (in *NodePool) Validate(_ context.Context) (errs *apis.FieldError) {
	return errs.Also(
		apis.ValidateObjectMetadata(in).ViaField("metadata"),
		in.Spec.validate().ViaField("spec"),
	)
}

func (in *NodePoolSpec) validate() (errs *apis.FieldError) {
	return errs.Also(
		in.Template.validate().ViaField("template"),
		in.Deprovisioning.validate().ViaField("deprovisioning"),
	)
}

func (in *NodeClaimTemplate) validate() (errs *apis.FieldError) {
	return errs.Also(
		in.validateLabels().ViaField("metadata"),
		in.Spec.validate().ViaField("spec"),
	)
}

func (in *NodeClaimTemplate) validateLabels() (errs *apis.FieldError) {
	for key, value := range in.Labels {
		if key == NodePoolLabelKey {
			errs = errs.Also(apis.ErrInvalidKeyName(key, "labels", "restricted"))
		}
		for _, err := range validation.IsQualifiedName(key) {
			errs = errs.Also(apis.ErrInvalidKeyName(key, "labels", err))
		}
		for _, err := range validation.IsValidLabelValue(value) {
			errs = errs.Also(apis.ErrInvalidValue(fmt.Sprintf("%s, %s", value, err), fmt.Sprintf("labels[%s]", key)))
		}
		if err := IsRestrictedLabel(key); err != nil {
			errs = errs.Also(apis.ErrInvalidKeyName(key, "labels", err.Error()))
		}
	}
	return errs
}

func (in *Deprovisioning) validate() (errs *apis.FieldError) {
	if in.ExpirationTTL.Duration < 0 {
		return errs.Also(apis.ErrInvalidValue("cannot be negative", "expirationTTL"))
	}
	if in.ConsolidationTTL.Duration < 0 {
		return errs.Also(apis.ErrInvalidValue("cannot be negative", "consolidationTTL"))
	}
	return errs
}

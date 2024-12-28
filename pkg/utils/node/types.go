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

package node

import (
	"errors"
	"fmt"
)

// NodeClaimNotFoundError is an error returned when no v1.NodeClaims are found matching the passed providerID
type NodeClaimNotFoundError struct {
	ProviderID string
}

func (e *NodeClaimNotFoundError) Error() string {
	return fmt.Sprintf("no nodeclaims found for provider id '%s'", e.ProviderID)
}

func IsNodeClaimNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	nnfErr := &NodeClaimNotFoundError{}
	return errors.As(err, &nnfErr)
}

func IgnoreNodeClaimNotFoundError(err error) error {
	if !IsNodeClaimNotFoundError(err) {
		return err
	}
	return nil
}

// DuplicateNodeClaimError is an error returned when multiple v1.NodeClaims are found matching the passed providerID
type DuplicateNodeClaimError struct {
	ProviderID string
}

func (e *DuplicateNodeClaimError) Error() string {
	return fmt.Sprintf("multiple nodeclaims found for provider id '%s'", e.ProviderID)
}

func IsDuplicateNodeClaimError(err error) bool {
	if err == nil {
		return false
	}
	dnErr := &DuplicateNodeClaimError{}
	return errors.As(err, &dnErr)
}

func IgnoreDuplicateNodeClaimError(err error) error {
	if !IsDuplicateNodeClaimError(err) {
		return err
	}
	return nil
}

type HydrationError struct{}

func (e *HydrationError) Error() string {
	return "resource has not been hydrated"
}

func IsHydrationError(err error) bool {
	if err == nil {
		return false
	}
	hErr := &HydrationError{}
	return errors.As(err, &hErr)
}

func IgnoreHydrationError(err error) error {
	if !IsHydrationError(err) {
		return err
	}
	return nil
}

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

package nodeclaim

import (
	"errors"
	"fmt"
)

// NodeNotFoundError is an error returned when no corev1.Nodes are found matching the passed providerID
type NodeNotFoundError struct {
	ProviderID string
}

func (e *NodeNotFoundError) Error() string {
	return fmt.Sprintf("no nodes found for provider id '%s'", e.ProviderID)
}

func IsNodeNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	nnfErr := &NodeNotFoundError{}
	return errors.As(err, &nnfErr)
}

func IgnoreNodeNotFoundError(err error) error {
	if !IsNodeNotFoundError(err) {
		return err
	}
	return nil
}

// DuplicateNodeError is an error returned when multiple corev1.Nodes are found matching the passed providerID
type DuplicateNodeError struct {
	ProviderID string
}

func (e *DuplicateNodeError) Error() string {
	return fmt.Sprintf("multiple found for provider id '%s'", e.ProviderID)
}

func IsDuplicateNodeError(err error) bool {
	if err == nil {
		return false
	}
	dnErr := &DuplicateNodeError{}
	return errors.As(err, &dnErr)
}

func IgnoreDuplicateNodeError(err error) error {
	if !IsDuplicateNodeError(err) {
		return err
	}
	return nil
}

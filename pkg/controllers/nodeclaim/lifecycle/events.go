/*
Copyright 2023 The Kubernetes Authors.

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

package lifecycle

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/apis/v1beta1"
	"sigs.k8s.io/karpenter/pkg/events"
)

func InsufficientCapacityErrorEvent(nodeClaim *v1beta1.NodeClaim, err error) events.Event {
	return events.Event{
		InvolvedObject: nodeClaim,
		Type:           v1.EventTypeWarning,
		Reason:         "InsufficientCapacityError",
		Message:        fmt.Sprintf("NodeClaim %s event: %s", nodeClaim.Name, truncateMessage(err.Error())),
		DedupeValues:   []string{string(nodeClaim.UID)},
	}
}

func NodeClassNotReadyEvent(nodeClaim *v1beta1.NodeClaim, err error) events.Event {
	return events.Event{
		InvolvedObject: nodeClaim,
		Type:           v1.EventTypeWarning,
		Reason:         "NodeClassNotReady",
		Message:        fmt.Sprintf("NodeClaim %s event: %s", nodeClaim.Name, truncateMessage(err.Error())),
		DedupeValues:   []string{string(nodeClaim.UID)},
	}
}

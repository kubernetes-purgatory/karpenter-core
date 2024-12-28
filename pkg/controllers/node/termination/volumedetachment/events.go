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

package volumedetachment

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"

	"sigs.k8s.io/karpenter/pkg/events"
)

func NodeAwaitingVolumeDetachmentEvent(node *corev1.Node, volumeAttachments ...*storagev1.VolumeAttachment) events.Event {
	return events.Event{
		InvolvedObject: node,
		Type:           corev1.EventTypeNormal,
		Reason:         "AwaitingVolumeDetachment",
		Message:        fmt.Sprintf("Awaiting deletion of %d VolumeAttachments bound to node", len(volumeAttachments)),
		DedupeValues:   []string{node.Name},
	}
}

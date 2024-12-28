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

package eviction

import (
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/events"
)

func PodEvictedEvent(pod *corev1.Pod) events.Event {
	return events.Event{
		InvolvedObject: pod,
		Type:           corev1.EventTypeNormal,
		Reason:         "Evicted",
		Message:        "Evicted pod",
		DedupeValues:   []string{pod.Namespace, pod.Name},
	}
}

func PodEvictionFailedEvent(pod *corev1.Pod, message string) events.Event {
	return events.Event{
		InvolvedObject: pod,
		Type:           corev1.EventTypeWarning,
		Reason:         "FailedEviction",
		Message:        message,
		DedupeValues:   []string{pod.Namespace, pod.Name},
	}
}

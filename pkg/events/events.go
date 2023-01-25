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

package events

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/flowcontrol"
)

// PodNominationRateLimiter is a pointer so it rate-limits across events
var PodNominationRateLimiter = flowcontrol.NewTokenBucketRateLimiter(5, 10)

func NominatePod(pod *v1.Pod, node *v1.Node) Event {
	return Event{
		InvolvedObject: pod,
		Type:           v1.EventTypeNormal,
		Reason:         "Nominated",
		Message:        fmt.Sprintf("Pod should schedule on %s", node.Name),
		DedupeValues:   []string{string(pod.UID), node.Name},
		RateLimiter:    PodNominationRateLimiter,
	}
}

func EvictPod(pod *v1.Pod) Event {
	return Event{
		InvolvedObject: pod,
		Type:           v1.EventTypeNormal,
		Reason:         "Evicted",
		Message:        "Evicted pod",
		DedupeValues:   []string{pod.Name},
	}
}

func PodFailedToSchedule(pod *v1.Pod, err error) Event {
	return Event{
		InvolvedObject: pod,
		Type:           v1.EventTypeWarning,
		Reason:         "FailedScheduling",
		Message:        fmt.Sprintf("Failed to schedule pod, %s", err),
		DedupeValues:   []string{string(pod.UID), err.Error()},
	}
}

func NodeFailedToDrain(node *v1.Node, err error) Event {
	return Event{
		InvolvedObject: node,
		Type:           v1.EventTypeWarning,
		Reason:         "FailedDraining",
		Message:        fmt.Sprintf("Failed to drain node, %s", err),
		DedupeValues:   []string{node.Name},
	}
}

func NodeInflightCheck(node *v1.Node, message string) Event {
	return Event{
		InvolvedObject: node,
		Type:           v1.EventTypeWarning,
		Reason:         "FailedInflightCheck",
		Message:        message,
		DedupeValues:   []string{node.Name, message},
	}
}

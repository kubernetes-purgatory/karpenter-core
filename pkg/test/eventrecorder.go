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

package test

import (
	"sync"

	"github.com/aws/karpenter-core/pkg/events"
)

var _ events.Recorder = (*EventRecorder)(nil)

// EventRecorder is a mock event recorder that is used to facilitate testing.
type EventRecorder struct {
	mu     sync.RWMutex
	calls  map[string]int
	events []events.Event
}

func NewEventRecorder() *EventRecorder {
	return &EventRecorder{
		calls: map[string]int{},
	}
}

func (e *EventRecorder) Publish(evt events.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, evt)
	e.calls[evt.Reason]++
}

func (e *EventRecorder) Calls(reason string) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.calls[reason]
}

func (e *EventRecorder) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = nil
	e.calls = map[string]int{}
}

func (e *EventRecorder) ForEachEvent(f func(evt events.Event)) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, e := range e.events {
		f(e)
	}
}

func (e *EventRecorder) DetectedEvent(msg string) bool {
	foundEvent := false
	e.ForEachEvent(func(evt events.Event) {
		if evt.Message == msg {
			foundEvent = true
		}
	})
	return foundEvent
}

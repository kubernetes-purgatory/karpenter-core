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

package scheduling

import (
	"container/heap"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestNodeClaimHeap_PopOrder(t *testing.T) {
	// Create NodeClaims with different pod counts
	nc1 := &NodeClaim{Pods: []*corev1.Pod{{}, {}}}     // 2 pods
	nc2 := &NodeClaim{Pods: []*corev1.Pod{{}}}         // 1 pod
	nc3 := &NodeClaim{Pods: []*corev1.Pod{{}, {}, {}}} // 3 pods
	nc4 := &NodeClaim{Pods: []*corev1.Pod{}}           // 0 pods

	// Initialize heap with NodeClaims
	h := NewNodeClaimHeap([]*NodeClaim{nc1, nc2, nc3, nc4})
	heap.Init(h)

	// Pop items and verify they come out in ascending order of pod count
	expected := []*NodeClaim{nc4, nc2, nc1, nc3}

	for i := 0; i < len(expected); i++ {
		item := heap.Pop(h).(*NodeClaim)
		assert.Equal(t, len(expected[i].Pods), len(item.Pods),
			"Expected NodeClaim with %d pods, got %d pods",
			len(expected[i].Pods), len(item.Pods))
	}

	// Verify heap is empty
	assert.Equal(t, 0, h.Len(), "Heap should be empty after popping all items")
}

func makeNodeClaims(size int) []*NodeClaim {
	nodeClaims := make([]*NodeClaim, 0, size)
	return lo.Map(nodeClaims, func(nc *NodeClaim, i int) *NodeClaim {
		podCount := rand.Intn(100)
		pods := lo.Map(lo.Range(podCount), func(_ int, j int) *corev1.Pod {
			return &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-" + strconv.Itoa(j)},
			}
		})
		return &NodeClaim{Pods: pods}
	})
}

func cloneNodeClaims(original []*NodeClaim) []*NodeClaim {
	return lo.Map(original, func(nc *NodeClaim, _ int) *NodeClaim {
		copiedPods := lo.Map(nc.Pods, func(pod *corev1.Pod, _ int) *corev1.Pod {
			return &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: pod.Name},
			}
		})
		return &NodeClaim{
			Pods: copiedPods,
			NodeClaimTemplate: NodeClaimTemplate{
				NodeClaim: v1.NodeClaim{
					Spec: v1.NodeClaimSpec{
						Taints: []corev1.Taint{
							{
								Key:    "custom-taint",
								Effect: corev1.TaintEffectNoSchedule,
								Value:  "custom-value",
							},
						},
					},
				},
			},
		}
	})
}

var nodeClaims = makeNodeClaims(3000)
var noTolerationPod = &corev1.Pod{
	Spec: corev1.PodSpec{
		Tolerations: nil,
	},
}

func BenchmarkHeapSorting(b *testing.B) {
	// clone the nodeClaims as identical test data for benchmark
	nodeClaims := cloneNodeClaims(nodeClaims)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := NodeClaimHeap(nodeClaims)
		heap.Init(&h)
		for h.Len() > 0 {
			nodeClaim := heap.Pop(&h).(*NodeClaim)
			_ = nodeClaim.Add(noTolerationPod, nil)
		}
	}
}

func BenchmarkSliceSorting(b *testing.B) {
	// clone the nodeClaims as identical test data for benchmark
	nodeClaims := cloneNodeClaims(nodeClaims)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sort.Slice(nodeClaims, func(a, b int) bool {
			return len(nodeClaims[a].Pods) < len(nodeClaims[b].Pods)
		})
		for _, nodeClaim := range nodeClaims {
			_ = nodeClaim.Add(noTolerationPod, nil)
		}
	}
}

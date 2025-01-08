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

import "container/heap"

type NodeClaimHeap []*NodeClaim

var (
	_ = heap.Interface(&NodeClaimHeap{}) // NodeClaimHeap is a standard heap
)

func NewNodeClaimHeap(nodeClaims []*NodeClaim) *NodeClaimHeap {
	h := NodeClaimHeap(nodeClaims)
	return &h
}

func (h NodeClaimHeap) Len() int           { return len(h) }
func (h NodeClaimHeap) Less(i, j int) bool { return len(h[i].Pods) < len(h[j].Pods) }
func (h NodeClaimHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *NodeClaimHeap) Push(x interface{}) {
	*h = append(*h, x.(*NodeClaim))
}

func (h *NodeClaimHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

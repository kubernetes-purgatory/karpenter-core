package ring

import (
	"sync"
)

type Buffer struct {
	mu       sync.RWMutex
	Contents []int
	Capacity int
	Read     int
	Write    int
}

func New(size int) *Buffer {
	return &Buffer{
		Contents: make([]int, size),
		Capacity: size,
		Read:     0,
		Write:    0,
	}
}

func (b *Buffer) Push(item int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Contents[b.Write] = item
	b.Write++
	b.Write %= b.Capacity // reset to 0 if we reach capacity
}

func (b *Buffer) Evaluate() (int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	score := 0
	for _, result := range b.Contents {
		score += result
	}
	// score can be interpreted by whatever is using the ring buffer
	return score, true
}

func (b *Buffer) Expire() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.Read == b.Write {
		b.Write++
		b.Write %= b.Capacity
	}
	b.Contents[b.Read] = -1
	b.Read++
	b.Read %= b.Capacity // reset to 0 if we reach capacity
	return
}

func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range b.Contents {
		b.Contents[i] = -1
	}
}

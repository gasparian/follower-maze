package queues

import (
	"sync"

	"github.com/gasparian/follower-maze/pkg/heap"
)

type BlockingPQueue[T any] struct {
	mx       *sync.RWMutex
	heap     *heap.Heap[T]
	count    uint64
	maxSize  uint64
	notEmpty *sync.Cond
	notFull  *sync.Cond
}

func NewPQueue[T any](comp func(a, b T) bool, maxSize uint64) *BlockingPQueue[T] {
	mx := &sync.RWMutex{}
	return &BlockingPQueue[T]{
		mx:       mx,
		heap:     heap.NewHeap[T](comp),
		maxSize:  maxSize,
		notEmpty: sync.NewCond(mx),
		notFull:  sync.NewCond(mx),
	}
}

func (p *BlockingPQueue[T]) Push(v T) {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.count == p.maxSize {
		p.notFull.Wait()
	}
	p.heap.Push(v)
	p.count++
	p.notEmpty.Signal()
}

func (p *BlockingPQueue[T]) Pop() T {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.count == 0 {
		p.notEmpty.Wait()
	}
	val := p.heap.Pop()
	p.count--
	p.notFull.Signal()
	return val
}

func (p *BlockingPQueue[T]) Len() int {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.heap.Len()
}

func (p *BlockingPQueue[T]) Clear() {
	p.mx.Lock()
	defer p.mx.Unlock()
	for i := uint64(0); i < p.count; i++ {
		p.heap.Pop()
	}
	p.count = uint64(0)
	p.notFull.Broadcast()
}

package pqueue

import (
	"container/heap"
	"sync"
)

type BlockingPQueue struct {
	mx       *sync.RWMutex
	items    heap.Interface
	count    uint64
	maxSize  uint64
	notEmpty *sync.Cond
	notFull  *sync.Cond
}

func New(h heap.Interface, maxSize uint64) *BlockingPQueue {
	mx := &sync.RWMutex{}
	return &BlockingPQueue{
		mx:       mx,
		items:    h,
		maxSize:  maxSize,
		notEmpty: sync.NewCond(mx),
		notFull:  sync.NewCond(mx),
	}
}

func (p *BlockingPQueue) Push(v interface{}) {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.count == p.maxSize {
		p.notFull.Wait()
	}
	heap.Push(p.items, v)
	p.count++
	p.notEmpty.Signal()
}

func (p *BlockingPQueue) Pop() interface{} {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.count == 0 {
		p.notEmpty.Wait()
	}
	val := heap.Pop(p.items)
	p.count--
	p.notFull.Signal()
	return val
}

func (p *BlockingPQueue) Len() int {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.items.Len()
}

func (p *BlockingPQueue) Clear() {
	p.mx.Lock()
	defer p.mx.Unlock()
	for i := uint64(0); i < p.count; i++ {
		p.items.Pop()
	}
	p.count = uint64(0)
	p.notFull.Broadcast()
}

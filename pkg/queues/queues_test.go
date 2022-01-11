package queues

import (
	"sync"
	"testing"
	"time"
)

const (
	timeoutMs = 500 * time.Millisecond
)

type intHeap []int

func (h intHeap) Len() int           { return len(h) }
func (h intHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h intHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *intHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *intHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func TestPQueuePushPop(t *testing.T) {
	pq := New(&intHeap{}, 3)
	wg := &sync.WaitGroup{}
	waitCh := make(chan bool)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pq.Push(1)
		}()
	}
	val := pq.Pop().(int)
	if val != 1 {
		t.Errorf(
			"Return value doesn't equal original one: should be 1, but got `%v`\n",
			val,
		)
	}
	wg.Wait()
	close(waitCh)

	select {
	case <-waitCh:
	case <-time.After(timeoutMs):
		t.Error("timeout")
	}
}

func TestPQueueClear(t *testing.T) {
	pq := New(&intHeap{}, 3)
	wg := &sync.WaitGroup{}
	waitCh := make(chan bool)
	for i := uint64(0); i < pq.maxSize*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pq.Push(1)
		}()
	}
	pq.Clear()
	wg.Wait()
	close(waitCh)

	select {
	case <-waitCh:
	case <-time.After(timeoutMs):
		t.Error("timeout")
	}
}

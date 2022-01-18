package queues

import (
	"sync"
	"testing"
	"time"
)

const (
	timeoutMs = 500 * time.Millisecond
)

func comp(a, b int) bool { return a < b }

func TestPQueuePushPop(t *testing.T) {
	pq := NewPQueue[int](comp, 3)
	wg := &sync.WaitGroup{}
	waitCh := make(chan bool)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pq.Push(1)
		}()
	}
	val := pq.Pop()
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
	pq := NewPQueue[int](comp, 3)
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

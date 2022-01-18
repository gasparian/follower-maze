package heap

import (
	"testing"
)

func TestHeap(t *testing.T) {
	h := NewHeap(func(a, b int) bool { return a < b })
	h.Push(1)
	h.Push(1)
	if h.Len() != 2 {
		t.Error()
	}
	v := h.Pop()
	if v != 1 {
		t.Error()
	}
}

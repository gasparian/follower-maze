package event

import (
	"container/heap"
	"errors"
	"strconv"
	"strings"
	"sync"
)

const (
	Follow       = 70
	Unfollow     = 85
	Broadcast    = 66
	PrivateMsg   = 80
	StatusUpdate = 83
)

var (
	badEventError = errors.New("Event contains less then 2 fields")
)

type Event struct {
	Raw        string
	Number     uint64
	MsgType    int
	FromUserID uint64
	ToUserID   uint64
}

func New(raw string) (*Event, error) {
	var fromUserID, toUserID uint64
	var parsed []string = strings.Split(raw, "|")
	var cleaned []string
	// defer log.Println("DEBUG: parsed event: ", raw, "; ", cleaned, fromUserID, toUserID)
	for _, p := range parsed {
		if len(p) > 0 {
			cleaned = append(cleaned, p)
		}
	}
	if len(cleaned) < 2 {
		return nil, badEventError
	}
	number, err := strconv.ParseUint(cleaned[0], 10, 64)
	if err != nil {
		return nil, err
	}
	if len(parsed) > 2 {
		fromUserID, err = strconv.ParseUint(cleaned[2], 10, 64)
	}
	if len(parsed) > 3 {
		toUserID, err = strconv.ParseUint(cleaned[3], 10, 64)
	}
	if err != nil {
		return nil, err
	}
	return &Event{
		Raw:        raw,
		Number:     number,
		MsgType:    int(cleaned[1][0]),
		FromUserID: fromUserID,
		ToUserID:   toUserID,
	}, nil
}

type EventsMinHeap []*Event

func (h EventsMinHeap) Len() int {
	return len(h)
}

func (h EventsMinHeap) Less(i, j int) bool {
	return h[i].Number < h[j].Number
}

func (h EventsMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *EventsMinHeap) Push(x interface{}) {
	*h = append(*h, x.(*Event))
}

func (h *EventsMinHeap) Pop() interface{} {
	old := *h
	tailIndex := old.Len() - 1
	tail := old[tailIndex]
	old[tailIndex] = nil
	*h = old[:tailIndex]
	return tail
}

type PriorityQueue struct {
	mx      sync.RWMutex
	events  *EventsMinHeap
	maxSize int
}

func NewPriorityQueue(maxSize int) *PriorityQueue {
	return &PriorityQueue{
		events:  new(EventsMinHeap),
		maxSize: maxSize,
	}
}

func (p *PriorityQueue) Push(event *Event) {
	p.mx.Lock()
	defer p.mx.Unlock()
	size := p.events.Len()
	if size == p.maxSize {
		heap.Pop(p.events)
	}
	heap.Push(
		p.events,
		event,
	)
}

func (p *PriorityQueue) Pop() *Event {
	p.mx.Lock()
	defer p.mx.Unlock()
	if p.events.Len() == 0 {
		return nil
	}
	return heap.Pop(p.events).(*Event)
}

func (p *PriorityQueue) Len() int {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.events.Len()
}

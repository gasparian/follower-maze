package main

import (
	"container/heap"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Follow       = 70
	Unfollow     = 85
	Broadcast    = 66
	PrivateMsg   = 80
	StatusUpdate = 83
)

type Event struct {
	Raw        string
	Number     uint64
	MsgType    int
	FromUserID uint64
	ToUserID   uint64
}

func NewEvent(raw string) (*Event, error) {
	var fromUserID, toUserID uint64
	var parsed []string = strings.Split(raw, "|")
	number, err := strconv.ParseUint(parsed[0], 10, 64)
	if err != nil {
		return nil, err
	}
	if len(parsed) == 3 {
		fromUserID, err = strconv.ParseUint(parsed[2], 10, 64)
	}
	if len(parsed) == 4 {
		toUserID, err = strconv.ParseUint(parsed[3], 10, 64)
	}
	if err != nil {
		return nil, err
	}
	return &Event{
		Raw:        raw,
		Number:     number,
		MsgType:    int(parsed[1][0]),
		FromUserID: fromUserID,
		ToUserID:   toUserID,
	}, nil
}

type Handler func(conn net.Conn, msgs []string)

type KVStore struct {
	mx    sync.RWMutex
	items map[uint64]interface{}
}

func NewKVStore() *KVStore {
	return &KVStore{
		items: make(map[uint64]interface{}),
	}
}

func (k *KVStore) Get(id uint64) interface{} {
	k.mx.RLock()
	defer k.mx.RUnlock()
	return k.items[id]
}

func (k *KVStore) Set(id uint64, val interface{}) {
	k.mx.Lock()
	defer k.mx.Unlock()
	k.items[id] = val
}

func (k *KVStore) Len() int {
	return len(k.items)
}

type KeysIterator chan uint64

func (it KeysIterator) Next() (uint64, bool) {
	id, opened := <-it
	if !opened {
		return 0, false
	}
	return id, true
}

func (k *KVStore) GetIterator() (KeysIterator, error) {
	k.mx.RLock()
	defer k.mx.RUnlock()

	it := make(KeysIterator, len(k.items))
	go func() {
		for k := range k.items {
			it <- k
		}
		close(it)
	}()
	return it, nil
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
	return heap.Pop(p.events).(*Event)
}

func (p *PriorityQueue) Size() int {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.events.Len()
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

type FollowerServer struct {
	mx                    sync.RWMutex
	clientsChans          *KVStore
	followers             *KVStore
	eventsQueue           *PriorityQueue
	clientsChanBufferSize int
	minQueueSize          int
	clientPort            string
	eventsPort            string
	queueTimeoutMs        time.Duration
	tcpReadTimeoutMs      time.Duration
	healthCheckTimeoutMs  time.Duration
}

func NewFollowerServer(clientPort, eventsPort string) *FollowerServer {
	return &FollowerServer{
		clientsChans:          NewKVStore(),
		followers:             NewKVStore(),
		eventsQueue:           NewPriorityQueue(10000),
		clientsChanBufferSize: 10,
		minQueueSize:          100,
		clientPort:            clientPort,
		eventsPort:            eventsPort,
		queueTimeoutMs:        1000 * time.Millisecond,
		tcpReadTimeoutMs:      20000 * time.Millisecond,
		healthCheckTimeoutMs:  100 * time.Millisecond,
	}
}

func (f *FollowerServer) GetClientsChanBufferSize() int {
	f.mx.RLock()
	defer f.mx.RUnlock()
	return f.clientsChanBufferSize
}

func (f *FollowerServer) GetTcpReadTimeoutMs() time.Duration {
	f.mx.RLock()
	defer f.mx.RUnlock()
	return f.tcpReadTimeoutMs
}

func (f *FollowerServer) GetHealthCheckTimeoutMs() time.Duration {
	f.mx.RLock()
	defer f.mx.RUnlock()
	return f.healthCheckTimeoutMs
}

func (f *FollowerServer) handle(conn net.Conn, msgProcessor Handler) {
	conn.SetReadDeadline(time.Now().Add(f.GetTcpReadTimeoutMs()))
	request := make([]byte, 128)
	defer conn.Close()
	for {
		read_len, err := conn.Read(request)

		if (err != nil) || (read_len == 0) {
			fmt.Fprint(os.Stderr, "Connection closed\n")
			break
		}

		req := strings.Fields(string(request[:read_len]))
		msgProcessor(conn, req)
	}
}

func (f *FollowerServer) startTCPServer(service string, msgProcessor Handler) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)
	log.Println("Starting tcp server...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go f.handle(conn, msgProcessor)
	}
}

func (f *FollowerServer) eventProcessor(conn net.Conn, req []string) {
	for _, r := range req {
		event, _ := NewEvent(r)
		f.eventsQueue.Push(event)
		log.Printf("%s -- %v -- %v\n", r, event, f.eventsQueue.Size())
	}
	log.Println()
}

func (f *FollowerServer) clientProcessor(conn net.Conn, req []string) {
	clientId, err := strconv.ParseUint(req[0], 10, 64)
	if err != nil {
		log.Println(err)
	}
	ch := make(chan *Event, f.GetClientsChanBufferSize())
	f.clientsChans.Set(clientId, ch)
	log.Println("New client connected: ", clientId, " ", f.clientsChans.Len())
	for {
		event, ok := <-ch
		if !ok {
			break
		}
		log.Printf("Client %v for event: %v\n", clientId, event.Raw)
		// TODO: check that client is alive and drop client id when the client is gone
	}
}

func (f *FollowerServer) eventsTransmitter() {
	var event *Event
	var it KeysIterator
	// var timerC <-chan time.Time
	f.mx.RLock()
	// queueTimeoutMs := f.queueTimeoutMs
	minQueueSize := f.minQueueSize
	f.mx.RUnlock()
	for {
		// TODO: use this timer
		// if f.eventsQueue.Size() == 0 {
		// 	timerC = time.NewTimer(queueTimeoutMs).C
		//  continue
		// }
		if f.eventsQueue.Size() >= minQueueSize {
			event = f.eventsQueue.Pop()
			// TODO: add conditions on each possible event state based on instructions set
			if event.FromUserID == 0 && event.ToUserID == 0 {
				it, _ = f.clientsChans.GetIterator()
				for id, ok := it.Next(); ok; {
					go func(id uint64) {
						val := f.clientsChans.Get(id)
						ch := val.(chan *Event)
						ch <- event
					}(id)
				}
			} else if event.MsgType == Follow && event.FromUserID > 0 && event.ToUserID > 0 {
				// f.followers.Set(event.ToUserID, )
			} else if event.MsgType == PrivateMsg && event.FromUserID > 0 && event.ToUserID > 0 {
				// ...
			} else if event.MsgType == StatusUpdate && event.FromUserID > 0 {
				// ...
			}
		}
	}
}

func (f *FollowerServer) Start() {
	go f.startTCPServer(f.clientPort, f.clientProcessor)
	go f.eventsTransmitter()
	go f.startTCPServer(f.eventsPort, f.eventProcessor)
	select {}
}

func main() {
	runtime.GOMAXPROCS(2)
	server := NewFollowerServer(":9099", ":9090")
	server.Start()
}

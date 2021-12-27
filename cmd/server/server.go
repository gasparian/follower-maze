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

var (
	once sync.Once // NOTE: for debug only
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
	if len(parsed) > 2 {
		fromUserID, err = strconv.ParseUint(parsed[2], 10, 64)
	}
	if len(parsed) > 3 {
		toUserID, err = strconv.ParseUint(parsed[3], 10, 64)
	}
	if err != nil {
		return nil, err
	}
	// log.Println("Parsed: ", parsed, fromUserID, toUserID)
	return &Event{
		Raw:        raw,
		Number:     number,
		MsgType:    int(parsed[1][0]),
		FromUserID: fromUserID,
		ToUserID:   toUserID,
	}, nil
}

type Handler interface {
	Handle(*FollowerServer, net.Conn, []string)
	GetTypeName() string
}

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

func (p *PriorityQueue) Len() int {
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
	mx                  sync.RWMutex
	clientsChans        *KVStore
	followers           *KVStore
	eventsQueue         *PriorityQueue
	minQueueSize        int
	maxBatchSizeBytes   int
	clientPort          string
	eventsPort          string
	queueTimeoutMs      time.Duration
	tcpReadTimeoutMs    time.Duration
	healthCheckPeriodMs time.Duration
}

type FollowerServerConfig struct {
	EventQueueMaxSize   int
	MinQueueSize        int
	ClientPort          string
	EventsPort          string
	QueueTimeoutMs      int
	TcpReadTimeoutMs    int
	HealthCheckPeriodMs int
	MaxBatchSizeBytes   int
}

func NewFollowerServer(config *FollowerServerConfig) *FollowerServer {
	return &FollowerServer{
		clientsChans:        NewKVStore(),
		followers:           NewKVStore(),
		eventsQueue:         NewPriorityQueue(config.EventQueueMaxSize),
		minQueueSize:        config.MinQueueSize,
		maxBatchSizeBytes:   config.MaxBatchSizeBytes,
		clientPort:          config.ClientPort,
		eventsPort:          config.EventsPort,
		queueTimeoutMs:      time.Duration(config.QueueTimeoutMs) * time.Millisecond,
		tcpReadTimeoutMs:    time.Duration(config.TcpReadTimeoutMs) * time.Millisecond,
		healthCheckPeriodMs: time.Duration(config.HealthCheckPeriodMs) * time.Millisecond,
	}
}

func (f *FollowerServer) GetTcpReadTimeoutMs() time.Duration {
	f.mx.RLock()
	defer f.mx.RUnlock()
	return f.tcpReadTimeoutMs
}

func (f *FollowerServer) handle(conn net.Conn, connHandler Handler) {
	conn.SetReadDeadline(time.Now().Add(f.GetTcpReadTimeoutMs()))
	request := make([]byte, f.maxBatchSizeBytes)
	defer conn.Close()
	for {
		read_len, err := conn.Read(request)
		if err != nil {
			conn.Close()
			log.Printf("Connection of type `%s` closed: %v\n", connHandler.GetTypeName(), err)
			// once.Do(func() {
			// 	log.Printf(">>> QUEUE:\n")
			// 	for f.eventsQueue.Len() > 0 {
			// 		log.Printf("%v ", f.eventsQueue.Pop())
			// 	}
			// })
			return
		}
		req := strings.Fields(string(request[:read_len]))
		connHandler.Handle(f, conn, req)
	}
}

func (f *FollowerServer) startTCPServer(service string, connHandler Handler) {
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
		go f.handle(conn, connHandler)
	}
}

type EventHandler string

func NewEventHandler() EventHandler {
	return EventHandler("EventHandler")
}

func (e EventHandler) GetTypeName() string {
	return string(e)
}

func (e EventHandler) Handle(followerServer *FollowerServer, conn net.Conn, req []string) {
	log.Println("Input events: ", req, "; Size: ", len(req))
	for _, r := range req {
		event, err := NewEvent(r)
		if err != nil {
			log.Println(err)
			continue
		}
		followerServer.eventsQueue.Push(event)
		// log.Printf("%s -- %v -- %v\n", r, event)
	}
}

type ClientHandler string

func NewClientHandler() ClientHandler {
	return ClientHandler("ClientHandler")
}

func (c ClientHandler) GetTypeName() string {
	return string(c)
}

func (c ClientHandler) Handle(followerServer *FollowerServer, conn net.Conn, req []string) {
	clientId, err := strconv.ParseUint(req[0], 10, 64)
	if err != nil {
		log.Println(err)
		return
	}
	ch := make(chan Event)
	followerServer.clientsChans.Set(clientId, ch)
	followerServer.followers.Set(clientId, make(map[uint64]bool))
	log.Println("New client connected: ", clientId)

	go func() {
		for {
			select {
			case event := <-ch:
				conn.Write([]byte(event.Raw + "\n"))
				// n, err := conn.Write([]byte(event.Raw + "\n"))
				// log.Println(">>>> WRITE: ", clientId, ", ", event.Number, "; ", n, ", ", err)
				// log.Printf("Client `%v` got event: `%v`\n", clientId, event.Raw)
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()
}

func (f *FollowerServer) transmitNextEvent() {
	event := *f.eventsQueue.Pop()
	log.Println(">>>>> <<<<<<: ", event.Raw)
	if event.MsgType == Broadcast {
		it, err := f.clientsChans.GetIterator()
		if err != nil {
			f.eventsQueue.Push(&event)
			return
		}
		log.Println(">>>> FLAG; BROADCAST", event.Number)
		for id, ok := it.Next(); ok; {
			go func(id uint64) {
				ch, ok := f.clientsChans.Get(id).(chan Event)
				if !ok {
					return
				}
				ch <- event
			}(id)
		}
	} else if event.MsgType == Follow && event.FromUserID > 0 && event.ToUserID > 0 {
		followers, ok := f.followers.Get(event.ToUserID).(map[uint64]bool)
		if !ok {
			f.eventsQueue.Push(&event)
			return
		}
		followers[event.FromUserID] = true
		f.followers.Set(event.ToUserID, followers)
		ch := f.clientsChans.Get(event.ToUserID).(chan Event)
		log.Println(">>>> FLAG; FOLLOW: ", event.Number, event.FromUserID, event.ToUserID)
		go func() {
			ch <- event
		}()
	} else if event.MsgType == PrivateMsg && event.FromUserID > 0 && event.ToUserID > 0 {
		ch, ok := f.clientsChans.Get(event.ToUserID).(chan Event)
		if !ok {
			f.eventsQueue.Push(&event)
			return
		}
		log.Println(">>>> FLAG; PRIVATE MSG: ", event.Number, event.FromUserID, event.ToUserID)
		go func() {
			ch <- event
		}()
	} else if event.MsgType == StatusUpdate && event.FromUserID > 0 {
		followers, ok := f.followers.Get(event.FromUserID).(map[uint64]bool)
		if !ok {
			f.eventsQueue.Push(&event)
			return
		}
		log.Println(">>>> FLAG; STATUS UPDATE: ", event.Number, event.FromUserID)
		for follower := range followers {
			go func(follower uint64) {
				ch, ok := f.clientsChans.Get(follower).(chan Event)
				if !ok {
					return
				}
				ch <- event
			}(follower)
		}
	} else if event.MsgType == Unfollow && event.FromUserID > 0 && event.ToUserID > 0 {
		followers, ok := f.followers.Get(event.ToUserID).(map[uint64]bool)
		if !ok {
			f.eventsQueue.Push(&event)
			return
		}
		log.Println(">>>> FLAG; UNFOLLOW: ", event.Number, event.FromUserID, event.ToUserID)
		delete(followers, event.FromUserID)
	}
}

func (f *FollowerServer) eventsTransmitter() {
	var timerC <-chan time.Time
	f.mx.RLock()
	queueTimeoutMs := f.queueTimeoutMs
	minQueueSize := f.minQueueSize
	f.mx.RUnlock()
	for {
		if f.eventsQueue.Len() == 0 {
			timerC = time.NewTimer(queueTimeoutMs).C
			continue
		}
		select {
		case <-timerC:
			f.transmitNextEvent()
		default:
			if f.eventsQueue.Len() >= minQueueSize {
				f.transmitNextEvent()
			}
		}
	}
}

func (f *FollowerServer) Start() {
	go f.startTCPServer(f.clientPort, NewClientHandler())
	go f.startTCPServer(f.eventsPort, NewEventHandler())
	go f.eventsTransmitter()
	select {}
}

func main() {
	runtime.GOMAXPROCS(2)
	server := NewFollowerServer(
		&FollowerServerConfig{
			EventQueueMaxSize:   10000,
			MinQueueSize:        4,
			MaxBatchSizeBytes:   1024,
			ClientPort:          ":9099",
			EventsPort:          ":9090",
			QueueTimeoutMs:      1000,
			TcpReadTimeoutMs:    20000,
			HealthCheckPeriodMs: 1000,
		},
	)
	server.Start()
}

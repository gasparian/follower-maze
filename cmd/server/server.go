package main

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"reflect"
	"runtime"
	"sort"
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
	once     sync.Once // NOTE: for debug only
	keyError = errors.New("Key has not been found")
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

func (k *KVStore) Del(id uint64) {
	k.mx.Lock()
	defer k.mx.Unlock()
	delete(k.items, id)
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

func (k *KVStore) GetIterator() KeysIterator {
	k.mx.RLock()
	defer k.mx.RUnlock()

	it := make(KeysIterator, len(k.items))
	go func() {
		for k := range k.items {
			it <- k
		}
		close(it)
	}()
	return it
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

type Node struct {
	Val  interface{}
	Next *Node
	Prev *Node
}

type BoundedFifoQueue struct {
	mx      sync.RWMutex
	Head    *Node
	Tail    *Node
	MaxSize int
	count   int
}

func NewBoundedQueue(maxSize int) *BoundedFifoQueue {
	return &BoundedFifoQueue{
		MaxSize: maxSize,
	}
}

func (b *BoundedFifoQueue) PushBack(val interface{}) {
	b.mx.Lock()
	defer b.mx.Unlock()
	if b.count == b.MaxSize {
		oldHead := b.Head
		b.Head = oldHead.Next
		oldHead = nil
		b.count--
	}
	newTail := &Node{Val: val, Prev: b.Tail}
	if b.count > 0 {
		oldTail := b.Tail
		oldTail.Next = newTail
		b.Tail = newTail
	} else {
		b.Head = newTail
	}
	b.Tail = newTail
	b.count++
}

func (b *BoundedFifoQueue) PushFront(val interface{}) {
	b.mx.Lock()
	defer b.mx.Unlock()
	if b.count == b.MaxSize {
		oldTail := b.Tail
		b.Tail = oldTail.Prev
		oldTail = nil
		b.count--
	}
	newHead := &Node{Val: val, Next: b.Head}
	if b.count > 0 {
		oldHead := b.Head
		oldHead.Prev = newHead
	} else {
		b.Tail = newHead
	}
	b.Head = newHead
	b.count++
}

func (b *BoundedFifoQueue) Pop() interface{} {
	b.mx.Lock()
	defer b.mx.Unlock()
	if b.count > 0 {
		head := b.Head
		b.Head = head.Next
		val := head.Val
		head = nil
		b.count--
		return val
	}
	return nil
}

func (b *BoundedFifoQueue) Front() interface{} {
	b.mx.RLock()
	defer b.mx.RUnlock()
	return b.Head.Val
}

func (b *BoundedFifoQueue) Back() interface{} {
	b.mx.RLock()
	defer b.mx.RUnlock()
	return b.Tail.Val
}

func (b *BoundedFifoQueue) Len() int {
	b.mx.RLock()
	defer b.mx.RUnlock()
	return b.count
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
	mx        sync.RWMutex
	clients   *KVStore
	followers *KVStore
	events    *BoundedFifoQueue
	// events             chan *Event
	// eventsFailed       chan *Event
	maxBatchSizeBytes  int
	eventsQueueMaxSize int
	clientPort         string
	eventsPort         string
	connDeadlineMs     time.Duration
}

type FollowerServerConfig struct {
	EventsQueueMaxSize int
	ClientPort         string
	EventsPort         string
	ConnDeadlineMs     int
	MaxBatchSizeBytes  int
}

func NewFollowerServer(config *FollowerServerConfig) *FollowerServer {
	return &FollowerServer{
		clients:   NewKVStore(),
		followers: NewKVStore(),
		events:    NewBoundedQueue(config.EventsQueueMaxSize),
		// events:             make(chan *Event, config.EventsQueueMaxSize),
		// eventsFailed:       make(chan *Event, config.EventsQueueMaxSize),
		maxBatchSizeBytes:  config.MaxBatchSizeBytes,
		eventsQueueMaxSize: config.EventsQueueMaxSize,
		clientPort:         config.ClientPort,
		eventsPort:         config.EventsPort,
		connDeadlineMs:     time.Duration(config.ConnDeadlineMs) * time.Millisecond,
	}
}

func (f *FollowerServer) GetConnDeadlineMs() time.Duration {
	f.mx.RLock()
	defer f.mx.RUnlock()
	return f.connDeadlineMs
}

func (f *FollowerServer) GetEventsQueueMaxSize() int {
	f.mx.RLock()
	defer f.mx.RUnlock()
	return f.eventsQueueMaxSize
}

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func (f *FollowerServer) handle(conn net.Conn, connHandler Handler) {
	conn.SetDeadline(time.Now().Add(f.GetConnDeadlineMs()))
	request := make([]byte, f.maxBatchSizeBytes)
	defer conn.Close()
	for {
		read_len, err := conn.Read(request)
		if err != nil {
			log.Printf("Connection of type `%s` closed: %v\n", connHandler.GetTypeName(), err)
			// once.Do(func() {
			// 	log.Printf(">>> QUEUE:\n")
			// 	for f.events.Len() > 0 {
			// 		log.Printf("%v ", f.events.Pop())
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
	// log.Println("Input events: ", req, "; Size: ", len(req))
	parsed := make([]*Event, 0)
	for _, r := range req {
		event, err := NewEvent(r)
		if err != nil {
			log.Println(err)
			continue
		}
		parsed = append(parsed, event)
	}
	sort.Slice(parsed, func(i, j int) bool {
		return parsed[i].Number < parsed[j].Number
	})
	// log.Printf("Parsed events: ")
	for _, p := range parsed {
		followerServer.events.PushBack(p)
		// log.Printf("%v ", p)
	}
	// log.Println()

	// go func() {
	// 	log.Printf("Parsed events: ")
	// 	for _, p := range parsed {
	// 		followerServer.events <- p
	// 		log.Printf("%v ", p)
	// 	}
	// 	log.Println()
	// }()
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
	pq := NewPriorityQueue(followerServer.GetEventsQueueMaxSize())
	followerServer.clients.Set(clientId, pq)
	followerServer.followers.Set(clientId, make(map[uint64]bool))
	log.Println("New client connected: ", clientId)

	go func() {
		var buff bytes.Buffer
		var event *Event
		for {
			if pq.Len() > 0 {
				event = pq.Pop()
				buff.WriteString(event.Raw)
				buff.WriteRune('\n')
				_, err := conn.Write(buff.Bytes())
				buff.Reset()
				if err != nil {
					followerServer.clients.Del(clientId)
					followerServer.followers.Del(clientId)
					log.Printf("Dropping client `%v` with error: %v", clientId, err)
					return
				}
				// log.Println(">>>> WRITE: ", clientId, ", ", event.Raw)
				// log.Printf("Client `%v` got event: `%v`\n", clientId, event.Raw)
				continue
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()
}

func (f *FollowerServer) transmitNextEvent(event *Event) error {
	// log.Println(">>>>> <<<<<<: ", event.Raw)
	if event.MsgType == Broadcast {
		it := f.clients.GetIterator()
		// log.Println(">>>> FLAG; BROADCAST", event.Number)
		// go func() {
		for id, ok := it.Next(); ok; {
			pq, ok := f.clients.Get(id).(*PriorityQueue)
			if !ok {
				return keyError
			}
			eventCpy := *event
			// BUG: Broadcasted the same event to the same client over and over again
			log.Println(">>>>>>>>>>>> FLAG ", id, "; ", event.Number)
			pq.Push(&eventCpy)
		}
		// }()
	} else if event.MsgType == Follow && event.FromUserID > 0 && event.ToUserID > 0 {
		followers, ok := f.followers.Get(event.ToUserID).(map[uint64]bool)
		if !ok {
			return keyError
		}
		followers[event.FromUserID] = true
		f.followers.Set(event.ToUserID, followers)
		pq := f.clients.Get(event.ToUserID).(*PriorityQueue)
		// log.Println(">>>> FLAG; FOLLOW: ", event.Number, event.FromUserID, event.ToUserID)
		eventCpy := *event
		pq.Push(&eventCpy)
	} else if event.MsgType == PrivateMsg && event.FromUserID > 0 && event.ToUserID > 0 {
		pq, ok := f.clients.Get(event.ToUserID).(*PriorityQueue)
		if !ok {
			return keyError
		}
		// log.Println(">>>> FLAG; PRIVATE MSG: ", event.Number, event.FromUserID, event.ToUserID)
		eventCpy := *event
		pq.Push(&eventCpy)
	} else if event.MsgType == StatusUpdate && event.FromUserID > 0 {
		followers, ok := f.followers.Get(event.FromUserID).(map[uint64]bool)
		if !ok {
			return keyError
		}
		// log.Println(">>>> FLAG; STATUS UPDATE: ", event.Number, event.FromUserID)
		// go func() {
		for follower := range followers {
			pq, ok := f.clients.Get(follower).(*PriorityQueue)
			if !ok {
				return keyError
			}
			eventCpy := *event
			pq.Push(&eventCpy)
		}
		// }()
	} else if event.MsgType == Unfollow && event.FromUserID > 0 && event.ToUserID > 0 {
		followers, ok := f.followers.Get(event.ToUserID).(map[uint64]bool)
		if !ok {
			return keyError
		}
		// log.Println(">>>> FLAG; UNFOLLOW: ", event.Number, event.FromUserID, event.ToUserID)
		delete(followers, event.FromUserID)
		f.followers.Set(event.ToUserID, followers) // TODO: need to re-set the map?
	}
	return nil
}

func (f *FollowerServer) eventsTransmitter() {
	var event *Event
	for {
		if f.events.Len() > 0 {
			event = f.events.Pop().(*Event)
			f.transmitNextEvent(event)
			// if err != nil {
			// 	f.events.PushFront(event)
			// }
			continue
		}
		time.Sleep(1 * time.Millisecond)
		// select {
		// case event := <-f.events:
		// 	err = f.transmitNextEvent(event)
		// 	if err != nil {
		// 		f.eventsFailed <- event
		// 	}
		// 	continue
		// case event := <-f.eventsFailed:
		// 	f.transmitNextEvent(event)
		// 	continue
		// default:
		// 	time.Sleep(1 * time.Millisecond)
		// }
	}
}

func (f *FollowerServer) Start() {
	go f.startTCPServer(f.clientPort, NewClientHandler())
	go f.startTCPServer(f.eventsPort, NewEventHandler())
	go f.eventsTransmitter()
	select {}
}

func main() {
	runtime.GOMAXPROCS(4)
	server := NewFollowerServer(
		&FollowerServerConfig{
			EventsQueueMaxSize: 10000,
			MaxBatchSizeBytes:  65536,
			ClientPort:         ":9099",
			EventsPort:         ":9090",
			ConnDeadlineMs:     20000,
		},
	)
	server.Start()
}

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

type Client struct {
	ID uint64
	Ch chan *Event
}

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

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

type FollowerServer struct {
	mx                 sync.RWMutex
	clients            *KVStore
	followers          *KVStore
	eventsChan         chan *Event
	clientsChan        chan *Client
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
		clients:            NewKVStore(),
		followers:          NewKVStore(),
		eventsChan:         make(chan *Event, config.EventsQueueMaxSize),
		clientsChan:        make(chan *Client),
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

func (f *FollowerServer) handleClient(conn net.Conn) {
	request := make([]byte, f.maxBatchSizeBytes)
	defer conn.Close()
	for {
		read_len, err := conn.Read(request)
		if err != nil {
			log.Printf("Client connection closed: %v\n", err)
			return
		}
		req := strings.Fields(string(request[:read_len]))
		clientId, err := strconv.ParseUint(req[0], 10, 64)
		if err != nil {
			log.Println(err)
			return
		}
		client := &Client{
			ID: clientId,
			Ch: make(chan *Event),
		}
		f.clientsChan <- client
		var buff bytes.Buffer
		var event *Event
		for {
			select {
			case event = <-client.Ch:
				buff.WriteString(event.Raw)
				buff.WriteRune('\n')
				_, err := conn.Write(buff.Bytes())
				buff.Reset()
				if err != nil {
					f.clients.Del(clientId)
					f.followers.Del(clientId)
					log.Printf("Dropping client `%v` with error: %v", clientId, err)
					return
				}
				log.Println(">>>> WRITE: ", clientId, ", ", event.Raw)
				// log.Printf("Client `%v` got event: `%v`\n", clientId, event.Raw)
				continue
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

func (f *FollowerServer) handleEvents(conn net.Conn) {
	request := make([]byte, f.maxBatchSizeBytes)
	defer conn.Close()
	for {
		read_len, err := conn.Read(request)
		if err != nil {
			log.Printf("Events connection closed: %v\n", err)
			return
		}
		parsed := make([]*Event, 0)
		req := strings.Fields(string(request[:read_len]))
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
		for _, p := range parsed {
			f.eventsChan <- p
		}
	}
}

func (f *FollowerServer) startTCPServer(service string, connHandler func(net.Conn)) {
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
		conn.SetDeadline(time.Now().Add(f.GetConnDeadlineMs()))
		go connHandler(conn)
	}
}

func (f *FollowerServer) transmitNextEvent(event *Event) error {
	// log.Println(">>>>> <<<<<<: ", event.Raw)
	if event.MsgType == Broadcast {
		it := f.clients.GetIterator()
		// log.Println(">>>> FLAG; BROADCAST", event.Number)
		wg := &sync.WaitGroup{}
		for {
			id, ok := it.Next()
			if !ok {
				break
			}
			pq, ok := f.clients.Get(id).(*PriorityQueue)
			if !ok {
				return keyError
			}
			eventCpy := *event
			wg.Add(1)
			go func(pq *PriorityQueue, event *Event) {
				defer wg.Done()
				pq.Push(event)
			}(pq, &eventCpy)
		}
		wg.Wait()
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
		wg := &sync.WaitGroup{}
		for follower := range followers {
			pq, ok := f.clients.Get(follower).(*PriorityQueue)
			if !ok {
				return keyError
			}
			eventCpy := *event
			wg.Add(1)
			go func(pq *PriorityQueue, event *Event) {
				defer wg.Done()
				pq.Push(&eventCpy)
			}(pq, &eventCpy)
		}
		wg.Wait()
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

func (f *FollowerServer) coordinator() {
	var event *Event
	var err error
	for {
		select {
		case event = <-f.eventsChan:
			err = f.transmitNextEvent(event)
			if err != nil {
				log.Printf("Error transmitting event: %v\n", err)
			}
			continue
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (f *FollowerServer) Start() {
	go f.startTCPServer(f.clientPort, f.handleClient)
	go f.startTCPServer(f.eventsPort, f.handleEvents)
	go f.coordinator()
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

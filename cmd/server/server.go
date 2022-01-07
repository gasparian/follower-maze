package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
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
	once sync.Once // NOTE: for debug only
)

type Event struct {
	Raw        string
	Number     uint64
	MsgType    int
	FromUserID uint64
	ToUserID   uint64
}

type ClientRequest struct {
	Payload  string
	Response chan error
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
		ch := make(chan *ClientRequest, 1)
		f.clients.Set(clientId, ch)
		f.followers.Set(clientId, make(map[uint64]bool))
		var buff bytes.Buffer
		var clientReq *ClientRequest
		for {
			select {
			case clientReq = <-ch:
				buff.WriteString(clientReq.Payload)
				buff.WriteRune('\n')
				_, err := conn.Write(buff.Bytes())
				buff.Reset()
				if err != nil {
					clientReq.Response <- err
					return
				}
				// DEBUG
				log.Println(">>>> WRITE: ", clientId, ", ", clientReq.Payload)
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

func (f *FollowerServer) sendEvent(clientId uint64, eventRaw string) {
	reqChan, ok := f.clients.Get(clientId).(chan *ClientRequest)
	if !ok {
		log.Printf("Error sending an event: client `%v` does not connected\n", clientId)
		return
	}
	req := &ClientRequest{
		Payload:  eventRaw,
		Response: make(chan error, 1),
	}
	reqChan <- req
	err := <-req.Response
	if err != nil {
		f.clients.Del(clientId)
		f.followers.Del(clientId)
		log.Printf("Dropping client `%v` with error: %v\n", clientId, err)
	}
}

func (f *FollowerServer) addFollower(clientId, followerId uint64) {
	followers, ok := f.followers.Get(clientId).(map[uint64]bool)
	if !ok {
		log.Printf("Error adding the follower: client `%v` does not connected\n", clientId)
		return
	}
	followers[followerId] = true
	// f.followers.Set(event.ToUserID, followers) // TODO: no need to re-set?
}

func (f *FollowerServer) removeFollower(clientId, followerId uint64) {
	followers, ok := f.followers.Get(clientId).(map[uint64]bool)
	if !ok {
		log.Printf("Error removing the follower: client `%v` does not connected\n", clientId)
		return
	}
	// log.Println(">>>> FLAG; UNFOLLOW: ", event.Number, event.FromUserID, event.ToUserID)
	delete(followers, followerId)
	// f.followers.Set(event.ToUserID, followers) // TODO: need to re-set the map?
}

func (f *FollowerServer) transmitNextEvent(event *Event) {
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
			eventCpy := (*event).Raw
			wg.Add(1)
			go func(clientId uint64, eventRaw string) {
				defer wg.Done()
				f.sendEvent(clientId, eventRaw)
			}(id, eventCpy)
		}
		wg.Wait()
	} else if event.MsgType == Follow && event.FromUserID > 0 && event.ToUserID > 0 {
		f.addFollower(event.ToUserID, event.FromUserID)
		f.sendEvent(event.ToUserID, event.Raw)
	} else if event.MsgType == PrivateMsg && event.FromUserID > 0 && event.ToUserID > 0 {
		f.sendEvent(event.ToUserID, event.Raw)
	} else if event.MsgType == StatusUpdate && event.FromUserID > 0 {
		followers, ok := f.followers.Get(event.FromUserID).(map[uint64]bool)
		if !ok {
			log.Printf("Error getting the followers: client `%v` does not connected\n", event.FromUserID)
			return
		}
		// log.Println(">>>> FLAG; STATUS UPDATE: ", event.Number, event.FromUserID)
		wg := &sync.WaitGroup{}
		for follower := range followers {
			wg.Add(1)
			eventCpy := (*event).Raw
			go func(clientId uint64, eventRaw string) {
				defer wg.Done()
				f.sendEvent(clientId, eventRaw)
			}(follower, eventCpy)
		}
		wg.Wait()
	} else if event.MsgType == Unfollow && event.FromUserID > 0 && event.ToUserID > 0 {
		f.removeFollower(event.ToUserID, event.FromUserID)
	}
}

func (f *FollowerServer) eventsTransmitter() {
	var event *Event
	for {
		select {
		case event = <-f.eventsChan:
			f.transmitNextEvent(event)
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (f *FollowerServer) Start() {
	go f.startTCPServer(f.clientPort, f.handleClient)
	go f.startTCPServer(f.eventsPort, f.handleEvents)
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

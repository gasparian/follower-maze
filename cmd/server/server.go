package main

import (
	"bytes"
	"errors"
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
	once          sync.Once // NOTE: for debug only
	badEventError = errors.New("Event contains less then 2 fields")
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

type Client struct {
	ID   uint64
	Chan chan *ClientRequest
}

func NewEvent(raw string) (*Event, error) {
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

func (f *FollowerServer) GetMaxBatchSize() int {
	f.mx.RLock()
	defer f.mx.RUnlock()
	return f.maxBatchSizeBytes
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
	request := make([]byte, f.GetMaxBatchSize())
	defer conn.Close()
	read_len, err := conn.Read(request)
	if err != nil {
		log.Printf("INFO: Client connection closed: %v\n", err)
		return
	}
	req := strings.Fields(string(request[:read_len]))
	clientId, err := strconv.ParseUint(req[0], 10, 64)
	if err != nil {
		log.Printf("ERROR: adding new client: %v", err)
		return
	}
	client := &Client{
		ID:   clientId,
		Chan: make(chan *ClientRequest),
	}
	// f.registerClient(client)
	f.clientsChan <- client
	log.Printf("INFO: Client `%v` connected\n", clientId)

	var buff bytes.Buffer
	var clientReq *ClientRequest
	timerChan := time.After(f.GetConnDeadlineMs())
	defer log.Printf("INFO: client `%v` is idle - disconnecting\n", clientId)
	for {
		select {
		case clientReq = <-client.Chan:
			buff.WriteString(clientReq.Payload)
			buff.WriteRune('\n')
			_, err := conn.Write(buff.Bytes())
			buff.Reset()
			clientReq.Response <- err
			if err != nil {
				return
			}
			// log.Println("DEBUG: >>>> WROTE: ", clientId, ", ", clientReq.Payload)
		case <-timerChan: // TODO: drop it
			return
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// TODO: read to buffer and then search for the delimiter
//       and sort only certain batch size (like 100)
//       if buffer ends not as \n delimiter -
//       keep this part and concatenate it with the next
//       chunk of data
func (f *FollowerServer) handleEvents(conn net.Conn) {
	request := make([]byte, f.GetMaxBatchSize())
	defer conn.Close()
	for {
		read_len, err := conn.Read(request)
		if err != nil {
			log.Printf("INFO: Events connection closed: %v\n", err)
			return
		}
		parsed := make([]*Event, 0)
		req := strings.Fields(string(request[:read_len]))
		for _, r := range req {
			event, err := NewEvent(r)
			if err != nil {
				log.Printf("ERROR: processing event `%v`: `%v`\n", r, err)
				continue
			}
			parsed = append(parsed, event)
		}
		sort.Slice(parsed, func(i, j int) bool {
			return parsed[i].Number < parsed[j].Number
		})
		log.Printf("DEBUG: read %v bytes; parsed %v events\n", read_len, len(parsed))
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
	log.Println("INFO: Starting tcp server...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		conn.SetDeadline(time.Now().Add(f.GetConnDeadlineMs()))
		go connHandler(conn)
	}
}

func (f *FollowerServer) registerClient(client *Client) {
	f.clients.Set(client.ID, client.Chan)
	f.followers.Set(client.ID, make(map[uint64]bool))
}

func (f *FollowerServer) sendEvent(clientId uint64, eventRaw string) {
	reqChan, ok := f.clients.Get(clientId).(chan *ClientRequest)
	if !ok {
		log.Printf("ERROR: trying to send an event: client `%v` does not connected\n", clientId)
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
		log.Printf("INFO: Dropping client `%v` with error: %v\n", clientId, err)
	}
}

func (f *FollowerServer) addFollower(clientId, followerId uint64) {
	followers, ok := f.followers.Get(clientId).(map[uint64]bool)
	if !ok {
		log.Printf("ERROR: adding the follower: client `%v` does not connected\n", clientId)
		return
	}
	followers[followerId] = true
	// f.followers.Set(event.ToUserID, followers) // TODO: no need to re-set?
}

func (f *FollowerServer) removeFollower(clientId, followerId uint64) {
	followers, ok := f.followers.Get(clientId).(map[uint64]bool)
	if !ok {
		log.Printf("ERROR: removing the follower: client `%v` does not connected\n", clientId)
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
			log.Printf("ERROR: getting the followers: client `%v` does not connected\n", event.FromUserID)
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

func (f *FollowerServer) coordinator() {
	var event *Event
	for {
		select {
		case client := <-f.clientsChan:
			f.registerClient(client)
		default:
		}
		select {
		case event = <-f.eventsChan:
			f.transmitNextEvent(event)
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// TODO: make handlers as abstractions and split them from the FollowerServer object (provide only interface)
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

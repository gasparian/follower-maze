package server

import (
	"bytes"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gasparian/follower-maze/internal/event"
	"github.com/gasparian/follower-maze/internal/follower"
	pqueue "github.com/gasparian/follower-maze/pkg/blocking-pqueue"
	kv "github.com/gasparian/follower-maze/pkg/kvstore"
	ss "github.com/gasparian/follower-maze/pkg/socket-server"
)

type FollowerServer struct {
	mx                 sync.RWMutex
	clientServer       ss.SocketServer
	eventsServer       ss.SocketServer
	clients            kv.KVStore
	followers          kv.KVStore
	clientsChan        chan *follower.Client
	maxBuffSizeBytes   int
	eventsQueueMaxSize int
	eventsPQueue       *pqueue.BlockingPQueue
}

type Config struct {
	EventsQueueMaxSize int
	ClientPort         string
	EventsPort         string
	MaxBuffSizeBytes   int
}

func New(config *Config) *FollowerServer {
	return &FollowerServer{
		clientServer:       ss.NewTCPServer(config.ClientPort),
		eventsServer:       ss.NewTCPServer(config.EventsPort),
		clients:            make(kv.KVStore),
		followers:          make(kv.KVStore),
		eventsPQueue:       pqueue.New(&event.EventsMinHeap{}, uint64(config.EventsQueueMaxSize)),
		clientsChan:        make(chan *follower.Client),
		maxBuffSizeBytes:   config.MaxBuffSizeBytes,
		eventsQueueMaxSize: config.EventsQueueMaxSize,
	}
}

func (f *FollowerServer) GetMaxBuffSize() int {
	f.mx.RLock()
	defer f.mx.RUnlock()
	return f.maxBuffSizeBytes
}

func (f *FollowerServer) GetEventsQueueMaxSize() int {
	f.mx.RLock()
	defer f.mx.RUnlock()
	return f.eventsQueueMaxSize
}

func (f *FollowerServer) handleClient(conn net.Conn) {
	buff := make([]byte, f.GetMaxBuffSize())
	read_len, err := conn.Read(buff)
	if err != nil {
		log.Printf("INFO: Client connection closed: %v\n", err)
		return
	}
	req := strings.Fields(string(buff[:read_len]))
	clientId, err := strconv.ParseUint(req[0], 10, 64)
	if err != nil {
		log.Printf("ERROR: adding new client: %v", err)
		return
	}
	c := &follower.Client{
		ID:   clientId,
		Chan: make(chan *follower.Request),
	}
	f.clientsChan <- c
	log.Printf("INFO: Client `%v` connected\n", clientId)

	var clientReqBuff bytes.Buffer
	var clientReq *follower.Request
	for {
		select {
		case clientReq = <-c.Chan:
			clientReqBuff.WriteString(clientReq.Payload)
			clientReqBuff.WriteRune('\n')
			_, err := conn.Write(clientReqBuff.Bytes())
			clientReqBuff.Reset()
			clientReq.Response <- err
			if err != nil {
				return
			}
			// log.Println("DEBUG: >>>> WROTE: ", clientId, ", ", clientReq.Payload)
		default:
			err := ss.ConnCheck(conn)
			if err != nil {
				log.Printf("INFO: Client `%v` disconnected: %v\n", clientId, err)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (f *FollowerServer) handleEvents(conn net.Conn) {
	buff := make([]byte, f.GetMaxBuffSize())
	var partialEvents strings.Builder
	for {
		read_len, err := conn.Read(buff)
		if err != nil {
			log.Printf("INFO: Events connection closed: %v\n", err)
			return
		}
		if read_len == 0 {
			continue
		}
		parsed := make([]*event.Event, 0)
		partialEvents.WriteString(string(buff[:read_len]))
		str := partialEvents.String()
		partialEvents.Reset()
		batch := strings.Fields(str)
		if str[len(str)-1] != '\n' {
			partialEvents.WriteString(batch[len(batch)-1])
			batch = batch[:len(batch)-1]
		}
		for _, e := range batch {
			event, err := event.New(e)
			if err != nil {
				log.Printf("ERROR: processing event `%v`: `%v`\n", e, err)
				continue
			}
			parsed = append(parsed, event)
		}
		log.Printf("DEBUG: read %v bytes; parsed %v events\n", read_len, len(parsed))
		for _, p := range parsed {
			f.eventsPQueue.Push(p)
		}
	}
}

func (f *FollowerServer) registerClient(c *follower.Client) {
	f.clients[c.ID] = c.Chan
	f.followers[c.ID] = make(map[uint64]bool)
}

func (f *FollowerServer) sendEvent(clientId uint64, eventRaw string) {
	reqChan, ok := f.clients[clientId].(chan *follower.Request)
	if !ok {
		// log.Printf("ERROR: trying to send an event: client `%v` does not connected\n", clientId)
		return
	}
	req := &follower.Request{
		Payload:  eventRaw,
		Response: make(chan error, 1),
	}
	reqChan <- req
	err := <-req.Response
	if err != nil {
		delete(f.clients, clientId)
		delete(f.followers, clientId)
		log.Printf("INFO: Dropping client `%v` with error: %v\n", clientId, err)
	}
}

func (f *FollowerServer) addFollower(clientId, followerId uint64) {
	followers, ok := f.followers[clientId].(map[uint64]bool)
	if !ok {
		// log.Printf("ERROR: adding the follower: client `%v` does not connected\n", clientId)
		return
	}
	followers[followerId] = true
	// f.followers.Set(event.ToUserID, followers) // TODO: no need to re-set?
}

func (f *FollowerServer) removeFollower(clientId, followerId uint64) {
	followers, ok := f.followers[clientId].(map[uint64]bool)
	if !ok {
		// log.Printf("ERROR: removing the follower: client `%v` does not connected\n", clientId)
		return
	}
	delete(followers, followerId)
	// f.followers.Set(event.ToUserID, followers) // TODO: need to re-set the map?
}

func (f *FollowerServer) transmitNextEvent(e *event.Event) {
	// log.Println(">>>>> <<<<<<: ", event.Raw)
	if e.MsgType == event.Broadcast {
		it := f.clients.GetIterator()
		// log.Println(">>>> FLAG; BROADCAST", event.Number)
		wg := &sync.WaitGroup{}
		for {
			id, ok := it.Next()
			if !ok {
				break
			}
			eventCpy := (*e).Raw
			wg.Add(1)
			go func(clientId uint64, eventRaw string) {
				defer wg.Done()
				f.sendEvent(clientId, eventRaw)
			}(id, eventCpy)
		}
		wg.Wait()
	} else if e.MsgType == event.Follow && e.FromUserID > 0 && e.ToUserID > 0 {
		f.addFollower(e.ToUserID, e.FromUserID)
		f.sendEvent(e.ToUserID, e.Raw)
	} else if e.MsgType == event.PrivateMsg && e.FromUserID > 0 && e.ToUserID > 0 {
		f.sendEvent(e.ToUserID, e.Raw)
	} else if e.MsgType == event.StatusUpdate && e.FromUserID > 0 {
		followers, ok := f.followers[e.FromUserID].(map[uint64]bool)
		if !ok {
			// log.Printf("ERROR: getting the followers: client `%v` does not connected\n", e.FromUserID)
			return
		}
		// log.Println(">>>> FLAG; STATUS UPDATE: ", event.Number, event.FromUserID)
		wg := &sync.WaitGroup{}
		for fl := range followers {
			wg.Add(1)
			eventCpy := (*e).Raw
			go func(clientId uint64, eventRaw string) {
				defer wg.Done()
				f.sendEvent(clientId, eventRaw)
			}(fl, eventCpy)
		}
		wg.Wait()
	} else if e.MsgType == event.Unfollow && e.FromUserID > 0 && e.ToUserID > 0 {
		f.removeFollower(e.ToUserID, e.FromUserID)
	}
}

func (f *FollowerServer) coordinator() {
	var e *event.Event
	var client *follower.Client
	for {
		select {
		case client = <-f.clientsChan:
			f.registerClient(client)
		default:
		}
		e = f.eventsPQueue.Pop().(*event.Event) // NOTE: will block if the queue is empty
		f.transmitNextEvent(e)
	}
}

// TODO: make handlers abstract and split them from the FollowerServer object (provide only interface)
func (f *FollowerServer) Start() {
	go f.clientServer.Start(f.handleClient)
	go f.eventsServer.Start(f.handleEvents)
	go f.coordinator()
	select {}
}

package server

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/gasparian/follower-maze/internal/event"
	"github.com/gasparian/follower-maze/internal/follower"
)

type EventsServer[T any] interface {
	GetNextEvent() T
	Start()
	Stop()
}

type FollowerServer struct {
	mx           sync.RWMutex
	clientServer EventsServer[*follower.Client]
	eventsServer EventsServer[*event.Event]
	clients      map[uint64]chan *follower.Request
	followers    map[uint64]map[uint64]bool
	stopSignal   chan bool
	stoppedFlag  int32
}

func NewFollowerServer(
	clientServer EventsServer[*follower.Client],
	eventsServer EventsServer[*event.Event],
) *FollowerServer {
	fs := &FollowerServer{}
	fs.clientServer = clientServer
	fs.eventsServer = eventsServer
	fs.clients = make(map[uint64]chan *follower.Request)
	fs.followers = make(map[uint64]map[uint64]bool)
	fs.stopSignal = make(chan bool)
	return fs
}

func (fs *FollowerServer) addFollower(clientID, followerId uint64) {
	_, ok := fs.clients[followerId]
	if !ok {
		// log.Printf("ERROR: adding the follower: client `%v` does not connected\n", clientID)
		return
	}
	followers, ok := fs.followers[clientID]
	if !ok {
		followers = make(map[uint64]bool)
		fs.followers[clientID] = followers
	}
	followers[followerId] = true
}

func (fs *FollowerServer) removeFollower(clientID, followerId uint64) {
	followers, ok := fs.followers[clientID]
	if !ok {
		// log.Printf("ERROR: removing the follower: client `%v` does not connected\n", clientID)
		return
	}
	delete(followers, followerId)
}

func (fs *FollowerServer) processEvent(e *event.Event) {
	if e == nil {
		return
	}
	fs.mx.Lock()
	defer fs.mx.Unlock()
	if e.MsgType == event.ServerShutdown {
		fs.cleanState()
	} else if e.MsgType == event.Broadcast {
		// log.Println(">>>> FLAG; BROADCAST", event.Number)
		wg := &sync.WaitGroup{}
		for id := range fs.clients {
			eventCpy := (*e).Raw
			wg.Add(1)
			go func(clientID uint64, eventRaw string) {
				defer wg.Done()
				fs.sendEvent(clientID, eventRaw)
			}(id, eventCpy)
		}
		wg.Wait()
	} else if e.MsgType == event.Follow && e.FromUserID > 0 && e.ToUserID > 0 {
		fs.addFollower(e.ToUserID, e.FromUserID)
		fs.sendEvent(e.ToUserID, e.Raw)
		// log.Println("DEBUG: FOLLOW: ", e.Number, e.ToUserID, e.FromUserID)
	} else if e.MsgType == event.PrivateMsg && e.FromUserID > 0 && e.ToUserID > 0 {
		fs.sendEvent(e.ToUserID, e.Raw)
	} else if e.MsgType == event.StatusUpdate && e.FromUserID > 0 {
		followers, ok := fs.followers[e.FromUserID]
		// log.Println("DEBUG: STATUS UPDATE: ", e.Number, e.FromUserID, followers)
		if !ok {
			// log.Printf("ERROR: getting the followers: client `%v` does not connected\n", e.FromUserID)
			return
		}
		wg := &sync.WaitGroup{}
		for fl := range followers {
			wg.Add(1)
			eventCpy := (*e).Raw
			go func(clientID uint64, eventRaw string) {
				defer wg.Done()
				fs.sendEvent(clientID, eventRaw)
			}(fl, eventCpy)
		}
		wg.Wait()
	} else if e.MsgType == event.Unfollow && e.FromUserID > 0 && e.ToUserID > 0 {
		fs.removeFollower(e.ToUserID, e.FromUserID)
	}
}

func (fs *FollowerServer) registerClient(c *follower.Client) {
	if c == nil {
		return
	}
	fs.mx.Lock()
	defer fs.mx.Unlock()
	fs.clients[c.ID] = c.Chan
	fs.followers[c.ID] = make(map[uint64]bool)
}

func (fs *FollowerServer) dropClient(clientID uint64) {
	fs.mx.Lock()
	defer fs.mx.Unlock()
	delete(fs.clients, clientID)
}

func (fs *FollowerServer) dropFollowers(clientID uint64) {
	fs.mx.Lock()
	defer fs.mx.Unlock()
	delete(fs.followers, clientID)
}

func (fs *FollowerServer) dropClientsAll() {
	fs.mx.Lock()
	defer fs.mx.Unlock()
	for clientID := range fs.clients {
		delete(fs.followers, clientID)
	}
}

func (fs *FollowerServer) dropFollowersAll() {
	for id := range fs.followers {
		fs.dropFollowers(id)
	}
}

func (fs *FollowerServer) cleanState() {
	fs.dropClientsAll()
	fs.dropFollowersAll()
}

func (fs *FollowerServer) sendEvent(clientID uint64, eventRaw string) {
	reqChan, ok := fs.clients[clientID]
	if !ok {
		// log.Printf("ERROR: trying to send an event: client `%v` does not connected\n", clientID)
		return
	}
	req := &follower.Request{
		Payload:  eventRaw,
		Response: make(chan error),
	}
	reqChan <- req
	err := <-req.Response
	if err != nil {
		fs.dropClient(clientID)
		log.Printf("INFO: Dropping client `%v` with error: %v\n", clientID, err)
	}
}

func (fs *FollowerServer) isStopped() bool {
	stopped := atomic.LoadInt32(&fs.stoppedFlag)
	if stopped > 0 {
		return true
	}
	return false
}

func (fs *FollowerServer) listenClients() {
	var client *follower.Client
	for {
		if fs.isStopped() {
			return
		}
		client = fs.clientServer.GetNextEvent() // NOTE: blocking
		fs.registerClient(client)
	}
}

func (fs *FollowerServer) listenEvents() {
	var e *event.Event
	for {
		if fs.isStopped() {
			return
		}
		e = fs.eventsServer.GetNextEvent() // NOTE: blocking
		fs.processEvent(e)
	}
}

func (fs *FollowerServer) listenStopSignal() {
	<-fs.stopSignal
	fs.eventsServer.Stop()
	fs.clientServer.Stop()
	atomic.StoreInt32(&fs.stoppedFlag, 1)
	fs.stopSignal <- true
}

func (fs *FollowerServer) Start() {
	go fs.listenStopSignal()
	go fs.clientServer.Start()
	go fs.eventsServer.Start()
	go fs.listenClients()
	go fs.listenEvents()
	select {}
}

func (fs *FollowerServer) Stop() {
	if !fs.isStopped() {
		fs.stopSignal <- true
		<-fs.stopSignal
	}
}

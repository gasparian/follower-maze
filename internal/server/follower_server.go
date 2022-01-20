package server

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/gasparian/follower-maze/internal/event"
	"github.com/gasparian/follower-maze/internal/follower"
)

type EventsServer[T any] interface {
	GetMsg() T
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

func (fs *FollowerServer) addFollower(clientId, followerId uint64) {
	_, ok := fs.clients[followerId]
	if !ok {
		// log.Printf("ERROR: adding the follower: client `%v` does not connected\n", clientId)
		return
	}
	followers, ok := fs.followers[clientId]
	if !ok {
		followers = make(map[uint64]bool)
		fs.followers[clientId] = followers
	}
	followers[followerId] = true
	// f.followers.Set(event.ToUserID, followers) // TODO: no need to re-set?
}

func (fs *FollowerServer) removeFollower(clientId, followerId uint64) {
	followers, ok := fs.followers[clientId]
	if !ok {
		// log.Printf("ERROR: removing the follower: client `%v` does not connected\n", clientId)
		return
	}
	delete(followers, followerId)
	// f.followers.Set(event.ToUserID, followers) // TODO: need to re-set the map?
}

func (fs *FollowerServer) processEvent(e *event.Event) {
	if e.MsgType == event.ServerShutdown {
		fs.cleanState()
	} else if e.MsgType == event.Broadcast {
		// log.Println(">>>> FLAG; BROADCAST", event.Number)
		wg := &sync.WaitGroup{}
		for id := range fs.clients {
			eventCpy := (*e).Raw
			wg.Add(1)
			go func(clientId uint64, eventRaw string) {
				defer wg.Done()
				fs.sendEvent(clientId, eventRaw)
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
			go func(clientId uint64, eventRaw string) {
				defer wg.Done()
				fs.sendEvent(clientId, eventRaw)
			}(fl, eventCpy)
		}
		wg.Wait()
	} else if e.MsgType == event.Unfollow && e.FromUserID > 0 && e.ToUserID > 0 {
		fs.removeFollower(e.ToUserID, e.FromUserID)
	}
}

func (fs *FollowerServer) registerClient(c *follower.Client) {
	fs.clients[c.ID] = c.Chan
	fs.followers[c.ID] = make(map[uint64]bool)
}

func (fs *FollowerServer) dropClient(clientId uint64) {
	delete(fs.clients, clientId)
}

func (fs *FollowerServer) dropFollowers(clientId uint64) {
	delete(fs.followers, clientId)
}

func (fs *FollowerServer) dropClientsAll() {
	for id := range fs.clients {
		fs.dropClient(id)
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

func (fs *FollowerServer) sendEvent(clientId uint64, eventRaw string) {
	reqChan, ok := fs.clients[clientId]
	if !ok {
		// log.Printf("ERROR: trying to send an event: client `%v` does not connected\n", clientId)
		return
	}
	req := &follower.Request{
		Payload:  eventRaw,
		Response: make(chan error),
	}
	reqChan <- req
	err := <-req.Response
	if err != nil {
		fs.dropClient(clientId)
		log.Printf("INFO: Dropping client `%v` with error: %v\n", clientId, err)
	}
}

func (fs *FollowerServer) isStopped() bool {
	stopped := atomic.LoadInt32(&fs.stoppedFlag)
	if stopped > 0 {
		return true
	}
	return false
}

func (fs *FollowerServer) coordinator() {
	var e *event.Event
	for {
		if fs.isStopped() {
			return
		}
		client := fs.clientServer.GetMsg() // NOTE: non-blocking
		if client != nil {
			fs.registerClient(client)
		}
		e = fs.eventsServer.GetMsg() // NOTE: will block if the queue is empty
		fs.processEvent(e)
		// TODO: while blocking - no new clients could be connected ;(
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
	go fs.coordinator()
	select {}
}

func (fs *FollowerServer) Stop() {
	if !fs.isStopped() {
		fs.stopSignal <- true
		<-fs.stopSignal
	}
}

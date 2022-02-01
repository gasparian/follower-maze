package server

import (
	"sync"
	"sync/atomic"

	"github.com/gasparian/follower-maze/internal/event"
	"github.com/gasparian/follower-maze/internal/follower"
	"github.com/golang/glog"
)

// EventsServer describes interface of any events server
type EventsServer[T any] interface {
	// Get next event from the internal queue of parsed events
	GetNextEvent() T
	// Starts server
	Start()
	// Stops server
	Stop()
}

// FollowerServer holds logic for running follower server, which
// consists of events-parsing server which listens to stream of events
// from events source, and client-acceptor server which accepts clients
// connections and transmits events to them
type FollowerServer struct {
	mx           sync.RWMutex
	clientServer EventsServer[*follower.Client]
	eventsServer EventsServer[*event.Event]
	clients      map[uint64]chan *follower.Request
	followers    map[uint64]map[uint64]bool
	stopSignal   chan bool
	stoppedFlag  int32
}

// NewFollowerServer creates new instance of FollowerServer
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

func (fs *FollowerServer) registerClient(c *follower.Client) {
	if c == nil {
		return
	}
	fs.mx.Lock()
	defer fs.mx.Unlock()
	fs.clients[c.ID] = c.Chan
	fs.followers[c.ID] = make(map[uint64]bool)
}

// dropClient deletes client from both maps: with
// clients and it's followers
// should be protected by mutex
func (fs *FollowerServer) dropClient(clientID uint64) {
	delete(fs.clients, clientID)
	delete(fs.followers, clientID)
}

// sendEvent puts event in the client's channel
// should be protected by mutex
func (fs *FollowerServer) sendEvent(clientID uint64, eventRaw string) {
	reqChan, ok := fs.clients[clientID]
	if !ok {
		glog.V(1).Infof("DEBUG: trying to send an event: client `%v` does not connected\n", clientID)
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
		glog.V(0).Infof("WARNING: Dropping client `%v` with error: %v\n", clientID, err)
	}
}

func (fs *FollowerServer) cleanState() {
	fs.mx.Lock()
	defer fs.mx.Unlock()
	for clientID := range fs.clients {
		fs.dropClient(clientID)
	}
}

func (fs *FollowerServer) broadcast(e *event.Event) {
	glog.V(1).Infoln("DEBUG: BROADCAST", e.Number)
	fs.mx.Lock()
	defer fs.mx.Unlock()
	for clientID := range fs.clients {
		fs.sendEvent(clientID, e.Raw)
	}
}

// addFollower adds new follower to the followers map
// should be protected by mutex
func (fs *FollowerServer) addFollower(clientID, followerID uint64) {
	_, ok := fs.clients[followerID]
	if !ok {
		glog.V(1).Infof("DEBUG: adding the follower: client `%v` does not connected\n", clientID)
		return
	}
	followers, ok := fs.followers[clientID]
	if !ok {
		followers = make(map[uint64]bool)
		fs.followers[clientID] = followers
	}
	followers[followerID] = true
}

func (fs *FollowerServer) follow(e *event.Event) {
	glog.V(1).Infoln("DEBUG: FOLLOW: ", e.Number, e.ToUserID, e.FromUserID)
	fs.mx.Lock()
	defer fs.mx.Unlock()
	fs.addFollower(e.ToUserID, e.FromUserID)
	fs.sendEvent(e.ToUserID, e.Raw)
}

func (fs *FollowerServer) privateMsg(e *event.Event) {
	fs.mx.Lock()
	defer fs.mx.Unlock()
	fs.sendEvent(e.ToUserID, e.Raw)
}

func (fs *FollowerServer) statusUpdate(e *event.Event) {
	fs.mx.Lock()
	defer fs.mx.Unlock()
	followers, ok := fs.followers[e.FromUserID]
	glog.V(1).Infoln("DEBUG: STATUS UPDATE: ", e.Number, e.FromUserID, len(followers))
	if !ok {
		glog.V(1).Infof("DEBUG: getting the followers: client `%v` does not connected\n", e.FromUserID)
		return
	}
	for clientID := range followers {
		fs.sendEvent(clientID, e.Raw)
	}
}

func (fs *FollowerServer) removeFollower(clientID, followerID uint64) {
	fs.mx.Lock()
	defer fs.mx.Unlock()
	followers, ok := fs.followers[clientID]
	if !ok {
		glog.V(1).Infof("DEBUG: removing the follower: client `%v` does not connected\n", clientID)
		return
	}
	delete(followers, followerID)
}

// processEvent sends event to the clients channels
// depending on event type
func (fs *FollowerServer) processEvent(e *event.Event) {
	if e == nil {
		return
	}
	if e.MsgType == event.ServerShutdown {
		fs.cleanState()
	} else if e.MsgType == event.Broadcast {
		fs.broadcast(e)
	} else if e.MsgType == event.Follow && e.FromUserID > 0 && e.ToUserID > 0 {
		fs.follow(e)
	} else if e.MsgType == event.PrivateMsg && e.FromUserID > 0 && e.ToUserID > 0 {
		fs.privateMsg(e)
	} else if e.MsgType == event.StatusUpdate && e.FromUserID > 0 {
		fs.statusUpdate(e)
	} else if e.MsgType == event.Unfollow && e.FromUserID > 0 && e.ToUserID > 0 {
		fs.removeFollower(e.ToUserID, e.FromUserID)
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

// Starts FollowerServer, by starting listening to events source and clients
func (fs *FollowerServer) Start() {
	go fs.listenStopSignal()
	go fs.clientServer.Start()
	go fs.eventsServer.Start()
	go fs.listenClients()
	fs.listenEvents()
}

// Stop shuts down FollowerServer
func (fs *FollowerServer) Stop() {
	if !fs.isStopped() {
		fs.stopSignal <- true
		<-fs.stopSignal
	}
}

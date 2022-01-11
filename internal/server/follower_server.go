package server

import (
	"log"
	"sync"

	"github.com/gasparian/follower-maze/internal/event"
	"github.com/gasparian/follower-maze/internal/follower"
	kv "github.com/gasparian/follower-maze/pkg/kvstore"
	q "github.com/gasparian/follower-maze/pkg/queues"
)

type MsgServer interface {
	q.Queue
	Start()
	Stop()
}

type FollowerServer struct {
	mx           sync.RWMutex
	ClientServer MsgServer
	EventsServer MsgServer
	clients      kv.KVStore
	followers    kv.KVStore
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

func (f *FollowerServer) coordinator() {
	var e *event.Event
	for {
		client := f.ClientServer.Pop()
		if client != nil {
			f.registerClient(client.(*follower.Client))
		}
		e = f.EventsServer.Pop().(*event.Event) // NOTE: will block if the queue is empty
		f.transmitNextEvent(e)
	}
}

func (f *FollowerServer) initStores() {
	f.clients = make(kv.KVStore)
	f.followers = make(kv.KVStore)
}

func (f *FollowerServer) Start() {
	f.initStores()
	go f.ClientServer.Start()
	go f.EventsServer.Start()
	go f.coordinator()
	select {}
}

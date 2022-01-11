package server

import (
	"log"
	"net"
	"strings"

	"github.com/gasparian/follower-maze/internal/event"
	q "github.com/gasparian/follower-maze/pkg/queues"
	ss "github.com/gasparian/follower-maze/pkg/socket-server"
)

type EventsServer struct {
	MaxBuffSize int
	server      ss.SocketServer
	eventsQueue *q.BlockingPQueue
}

func NewEventsServer(maxBuffSize, eventsQueueMaxSize int, servicePort string) *EventsServer {
	return &EventsServer{
		MaxBuffSize: maxBuffSize,
		server:      ss.NewTCPServer(servicePort),
		eventsQueue: q.New(&event.EventsMinHeap{}, uint64(eventsQueueMaxSize)),
	}
}

func (ch *EventsServer) Push(val interface{}) {
	ch.eventsQueue.Push(val)
}

func (ch *EventsServer) Pop() interface{} {
	return ch.eventsQueue.Pop()
}

func (ch *EventsServer) Start() {
	ch.server.Start(ch.handler)
}

func (ch *EventsServer) Stop() {
	ch.server.Stop()
}

func (c *EventsServer) handler(conn net.Conn) {
	buff := make([]byte, c.MaxBuffSize)
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
			c.eventsQueue.Push(p)
		}
	}
}

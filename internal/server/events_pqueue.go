package server

import (
	"log"
	"net"
	"strings"
	"sync"

	"github.com/gasparian/follower-maze/internal/event"
	q "github.com/gasparian/follower-maze/pkg/queues"
	ss "github.com/gasparian/follower-maze/pkg/socket-server"
)

type EventsParserPQueue struct {
	mx                 sync.RWMutex
	maxBuffSize        int
	server             ss.SocketServer
	eventsQueue        *q.BlockingPQueue[*event.Event]
	largestEventNumber uint64
	shutdownEvent      *event.Event
}

func NewEventsParserPQueue(maxBuffSize, eventsQueueMaxSize int, servicePort string) *EventsParserPQueue {
	return &EventsParserPQueue{
		maxBuffSize: maxBuffSize,
		server:      ss.NewTCPServer(servicePort),
		eventsQueue: q.NewPQueue[*event.Event](
			func(a, b *event.Event) bool { return a.Number < b.Number },
			uint64(eventsQueueMaxSize),
		),
		shutdownEvent: event.ShutdownEvent,
	}
}

func (ep *EventsParserPQueue) GetMaxBuffSize() int {
	ep.mx.RLock()
	defer ep.mx.RUnlock()
	return ep.maxBuffSize
}

func (ep *EventsParserPQueue) GetMsg() *event.Event {
	return ep.eventsQueue.Pop()
}

func (ep *EventsParserPQueue) Start() {
	ep.server.Start(ep.handler)
}

func (ep *EventsParserPQueue) Stop() {
	ep.server.Stop()
}

func (ep *EventsParserPQueue) updateLargestEventNumber(n uint64) {
	if n > ep.largestEventNumber {
		ep.largestEventNumber = n
	}
}

func (ep *EventsParserPQueue) handler(conn net.Conn) {
	buff := make([]byte, ep.GetMaxBuffSize())
	var partialEvents strings.Builder
	for {
		read_len, err := conn.Read(buff)
		if err != nil {
			log.Printf("INFO: Events connection closed: %v\n", err)
			ep.shutdownEvent.Number = ep.largestEventNumber
			ep.eventsQueue.Push(ep.shutdownEvent)
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
			ev, err := event.New(e)
			if err != nil {
				log.Printf("ERROR: processing event `%v`: `%v`\n", e, err)
				continue
			}
			parsed = append(parsed, ev)
			ep.updateLargestEventNumber(ev.Number)
		}
		log.Printf("DEBUG: read %v bytes; parsed %v events\n", read_len, len(parsed))
		for _, p := range parsed {
			ep.eventsQueue.Push(p)
		}
	}
}

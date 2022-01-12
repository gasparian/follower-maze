package server

import (
	"log"
	"net"
	"strings"
	"time"

	"github.com/gasparian/follower-maze/internal/event"
	q "github.com/gasparian/follower-maze/pkg/queues"
	ss "github.com/gasparian/follower-maze/pkg/socket-server"
)

type EventsParserPQueue struct {
	maxBuffSize int
	server      ss.SocketServer
	eventsQueue *q.BlockingPQueue
}

func NewEventsParserPQueue(maxBuffSize, eventsQueueMaxSize int, servicePort string) *EventsParserPQueue {
	return &EventsParserPQueue{
		maxBuffSize: maxBuffSize,
		server:      ss.NewTCPServer(servicePort),
		eventsQueue: q.New(&event.EventsMinHeap{}, uint64(eventsQueueMaxSize)),
	}
}

func (ep *EventsParserPQueue) GetMsg() interface{} {
	return ep.eventsQueue.Pop()
}

func (ep *EventsParserPQueue) Start() {
	ep.server.Start(ep.handler)
}

func (ep *EventsParserPQueue) Stop() {
	ep.server.Stop()
}

func (ep *EventsParserPQueue) handler(conn net.Conn) {
	buff := make([]byte, ep.maxBuffSize)
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
			ep.eventsQueue.Push(p)
		}
	}
}

type EventsParserBatched struct {
	maxBuffSize   int
	maxBatchSize  int
	readTimeoutMs time.Duration
	server        ss.SocketServer
	eventsQueue   chan *event.Event
}

func NewEventsParserBatched(maxBuffSize, maxBatchSize, eventsQueueMaxSize, readTimeoutMs int, servicePort string) *EventsParserBatched {
	return &EventsParserBatched{
		maxBuffSize:   maxBuffSize,
		maxBatchSize:  maxBatchSize,
		readTimeoutMs: time.Duration(readTimeoutMs) * time.Millisecond,
		server:        ss.NewTCPServer(servicePort),
		eventsQueue:   make(chan *event.Event, eventsQueueMaxSize),
	}
}

func (ep *EventsParserBatched) GetMsg() interface{} {
	return <-ep.eventsQueue
}

func (ep *EventsParserBatched) Start() {
	ep.server.Start(ep.handler)
}

func (ep *EventsParserBatched) Stop() {
	ep.server.Stop()
}

func (ep *EventsParserBatched) handler(conn net.Conn) {
	buff := make([]byte, ep.maxBuffSize)
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
			ep.eventsQueue <- p
		}
	}
}

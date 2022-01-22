package server

import (
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gasparian/follower-maze/internal/event"
	ss "github.com/gasparian/follower-maze/pkg/socket-server"
)

type EventsParserBatched struct {
	mx            sync.RWMutex
	maxBuffSize   int
	maxBatchSize  int
	readTimeoutMs time.Duration
	server        ss.SocketServer
	eventsQueue   chan *event.Event
	shutdownEvent event.Event
}

func NewEventsParserBatched(maxBuffSize, maxBatchSize, eventsQueueMaxSize, readTimeoutMs int, servicePort string) *EventsParserBatched {
	return &EventsParserBatched{
		maxBuffSize:   maxBuffSize,
		maxBatchSize:  maxBatchSize,
		readTimeoutMs: time.Duration(readTimeoutMs) * time.Millisecond,
		server:        ss.NewTCPServer(servicePort),
		eventsQueue:   make(chan *event.Event, eventsQueueMaxSize),
		shutdownEvent: event.ShutdownEvent,
	}
}

func (ep *EventsParserBatched) GetNextMsg() *event.Event {
	return <-ep.eventsQueue
}

func (ep *EventsParserBatched) Start() {
	ep.server.Start(ep.handler)
}

func (ep *EventsParserBatched) Stop() {
	ep.server.Stop()
}

func (ep *EventsParserBatched) sortAndSend(batchParsed []*event.Event) []*event.Event {
	sort.Slice(batchParsed, func(i, j int) bool {
		return batchParsed[i].Number < batchParsed[j].Number
	})
	for _, e := range batchParsed {
		ep.eventsQueue <- e
	}
	return batchParsed[:0]
}

func (ep *EventsParserBatched) handler(conn net.Conn) {
	ep.mx.RLock()
	maxBuffSize := ep.maxBuffSize
	maxBatchSize := ep.maxBatchSize
	readTimeoutMs := ep.readTimeoutMs
	shutdownEvent := ep.shutdownEvent
	ep.mx.RUnlock()
	parsedEventsChan := make(chan *event.Event, maxBatchSize)
	go func() {
		buff := make([]byte, maxBuffSize)
		var partialEvents strings.Builder
		for {
			read_len, err := conn.Read(buff)
			if err != nil {
				log.Printf("INFO: Events connection closed: %v\n", err)
				parsedEventsChan <- &shutdownEvent
				return
			}
			partialEvents.WriteString(string(buff[:read_len]))
			str := partialEvents.String()
			partialEvents.Reset()
			batch := strings.Fields(str)
			if str[len(str)-1] != '\n' {
				partialEvents.WriteString(batch[len(batch)-1])
				batch = batch[:len(batch)-1]
			}
			log.Printf("DEBUG: read %v bytes; parsed %v events\n", read_len, len(batch))
			for _, ev := range batch {
				parsedEvent, err := event.NewEvent(ev)
				if err != nil {
					log.Printf("ERROR: processing event `%v`: `%v`\n", ev, err)
					continue
				}
				parsedEventsChan <- parsedEvent
			}
		}
	}()
	batchParsed := make([]*event.Event, 0)
	timer := time.After(readTimeoutMs)
	var parsedEvent *event.Event
	for {
		select {
		case parsedEvent = <-parsedEventsChan:
			if parsedEvent.MsgType == event.ServerShutdown {
				ep.eventsQueue <- parsedEvent
				return
			}
			batchParsed = append(batchParsed, parsedEvent)
			if len(batchParsed) == maxBatchSize {
				batchParsed = ep.sortAndSend(batchParsed)
			}
		case <-timer:
			if len(batchParsed) > 0 {
				batchParsed = ep.sortAndSend(batchParsed)
			}
			timer = time.After(readTimeoutMs)
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

package server

import (
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gasparian/follower-maze/internal/event"
	q "github.com/gasparian/follower-maze/pkg/queues"
	ss "github.com/gasparian/follower-maze/pkg/socket-server"
)

type StringStreamParser struct {
	partialEvents strings.Builder
	Delim         byte
}

func (sp *StringStreamParser) Parse(buff []byte) []string {
	sp.partialEvents.WriteString(string(buff))
	str := sp.partialEvents.String()
	sp.partialEvents.Reset()
	batch := strings.Fields(str)
	if str[len(str)-1] != sp.Delim {
		sp.partialEvents.WriteString(batch[len(batch)-1])
		batch = batch[:len(batch)-1]
	}
	return batch
}

type EventsParserPQueue struct {
	mx                 sync.RWMutex
	maxBuffSize        int
	server             ss.SocketServer
	eventsQueue        *q.BlockingPQueue[*event.Event]
	largestEventNumber uint64
	shutdownEvent      event.Event
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

func (ep *EventsParserPQueue) GetNextEvent() *event.Event {
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
	defer conn.Close()
	streamParser := StringStreamParser{Delim: '\n'}
	for {
		read_len, err := conn.Read(buff)
		if err != nil {
			log.Printf("INFO: Events connection closed: %v\n", err)
			ep.shutdownEvent.Number = ep.largestEventNumber
			ep.eventsQueue.Push(&ep.shutdownEvent)
			return
		}
		batch := streamParser.Parse(buff[:read_len])
		if len(batch) == 0 {
			continue
		}
		parsed := make([]*event.Event, 0)
		for _, e := range batch {
			ev, err := event.NewEvent(e)
			if err != nil {
				log.Printf("ERROR: processing event `%v`: `%v`\n", e, err)
				continue
			}
			parsed = append(parsed, ev)
		}
		sort.Slice(parsed, func(i, j int) bool {
			return parsed[i].Number < parsed[j].Number
		})
		ep.updateLargestEventNumber(parsed[len(parsed)-1].Number)
		for _, p := range parsed {
			ep.eventsQueue.Push(p)
		}
		log.Printf("DEBUG: read %v bytes\n", read_len)
	}
}

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

func (ep *EventsParserBatched) GetNextEvent() *event.Event {
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
	defer conn.Close()
	ep.mx.RLock()
	maxBuffSize := ep.maxBuffSize
	maxBatchSize := ep.maxBatchSize
	readTimeoutMs := ep.readTimeoutMs
	shutdownEvent := ep.shutdownEvent
	ep.mx.RUnlock()
	parsedEventsChan := make(chan *event.Event, maxBatchSize)
	go func() {
		defer conn.Close()
		buff := make([]byte, maxBuffSize)
		streamParser := StringStreamParser{Delim: '\n'}
		for {
			read_len, err := conn.Read(buff)
			if err != nil {
				log.Printf("INFO: Events connection closed: %v\n", err)
				parsedEventsChan <- &shutdownEvent
				return
			}
			batch := streamParser.Parse(buff[:read_len])
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

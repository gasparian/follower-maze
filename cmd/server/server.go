package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	// "bytes"
	// "encoding/binary"
	"time"
	// "unicode/utf8"
	"container/heap"
	"sync"
)

const (
	Follow       = 70
	Unfollow     = 85
	Broadcast    = 66
	PrivateMsg   = 80
	StatusUpdate = 83
)

var (
	clients     = InitClientsChans(10)
	eventsQueue = InitPriorityQueue()
)

type Event struct {
	Number     uint64
	MsgType    int
	FromUserID uint64
	ToUserID   uint64
}

type handler func(conn net.Conn)

type ClientsChans struct {
	mx         sync.RWMutex
	items      map[uint64]chan *Event
	bufferSize uint
}

func InitClientsChans(bufferSize uint) *ClientsChans {
	return &ClientsChans{
		items:      make(map[uint64]chan *Event),
		bufferSize: bufferSize,
	}
}

func (c *ClientsChans) InitChan(id uint64) chan *Event {
	c.mx.Lock()
	defer c.mx.Unlock()
	ch := make(chan *Event, c.bufferSize)
	c.items[id] = ch
	return ch
}

func (c *ClientsChans) GetChan(id uint64) chan *Event {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.items[id]
}

func (c *ClientsChans) Send(id uint64, event *Event) {
	ch := c.GetChan(id)
	ch <- event
}

func (c *ClientsChans) Size() int {
	return len(c.items)
}

type EventsMinHeap []*Event

func (h EventsMinHeap) Len() int {
	return len(h)
}

func (h EventsMinHeap) Less(i, j int) bool {
	return h[i].Number < h[j].Number
}

func (h EventsMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *EventsMinHeap) Push(x interface{}) {
	*h = append(*h, x.(*Event))
}

func (h *EventsMinHeap) Pop() interface{} {
	old := *h
	tailIndex := old.Len() - 1
	tail := old[tailIndex]
	old[tailIndex] = nil
	*h = old[:tailIndex]
	return tail
}

type PriorityQueue struct {
	mx     sync.RWMutex
	events *EventsMinHeap
}

func InitPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		events: new(EventsMinHeap),
	}
}

func (p *PriorityQueue) Push(event *Event) {
	p.mx.Lock()
	defer p.mx.Unlock()
	heap.Push(
		p.events,
		event,
	)
}

func (p *PriorityQueue) Pop() *Event {
	p.mx.Lock()
	defer p.mx.Unlock()
	return heap.Pop(p.events).(*Event)
}

func (p *PriorityQueue) Size() int {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return p.events.Len()
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func parsePayload(req string) (*Event, error) {
	var fromUserID, toUserID uint64
	var parsed []string = strings.Split(req, "|")
	number, err := strconv.ParseUint(parsed[0], 10, 64)
	if err != nil {
		return nil, err
	}
	if len(parsed) == 3 {
		fromUserID, err = strconv.ParseUint(parsed[2], 10, 64)
	}
	if len(parsed) == 4 {
		toUserID, err = strconv.ParseUint(parsed[3], 10, 64)
	}
	if err != nil {
		return nil, err
	}
	return &Event{
		Number:     number,
		MsgType:    int(parsed[1][0]),
		FromUserID: fromUserID,
		ToUserID:   toUserID,
	}, nil
}

func handleEvent(conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(20000 * time.Millisecond))
	request := make([]byte, 128)
	defer conn.Close()
	for {
		read_len, err := conn.Read(request)

		if (err != nil) || (read_len == 0) {
			fmt.Fprint(os.Stderr, "Connection closed")
			break
		}

		req := strings.Fields(string(request[:read_len]))
		for _, r := range req {
			event, _ := parsePayload(r)
			eventsQueue.Push(event)
			log.Printf("%s -- %v -- %v \n", r, event, eventsQueue.Size())
		}
		log.Println()
	}
}

func handleClient(conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(20000 * time.Millisecond))
	request := make([]byte, 128)
	defer conn.Close()
	for {
		read_len, err := conn.Read(request)

		if (err != nil) || (read_len == 0) {
			// fmt.Fprint(os.Stderr, "Connection closed")
			log.Println("Connection closed")
			break
		}

		req := strings.Fields(string(request[:read_len]))
		clientId, err := strconv.ParseUint(req[0], 10, 64)
		if err != nil {
			log.Println(err)
			continue
		}
		clients.InitChan(clientId)
		log.Println("New client connected: ", clientId, " ", clients.Size())
	}
}

func StartTCPServer(service string, handlerFunc handler) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)
	log.Println("Starting tcp server...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handlerFunc(conn)
	}
}

func main() {
	log.SetOutput(os.Stdout) // force log to stdout
	runtime.GOMAXPROCS(2)

	go StartTCPServer(":9099", handleClient)
	StartTCPServer(":9090", handleEvent)
}

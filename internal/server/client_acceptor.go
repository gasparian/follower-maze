package server

import (
	"bytes"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gasparian/follower-maze/internal/follower"
	ss "github.com/gasparian/follower-maze/pkg/socket-server"
)

type ClientAcceptor struct {
	mx                 sync.RWMutex
	maxBuffSizeBytes   int
	server             ss.SocketServer
	clientsChan        chan *follower.Client
	eventsQueueMaxSize int
}

func NewClientAcceptor(maxBuffSizeBytes, eventsQueueMaxSize int, servicePort string) *ClientAcceptor {
	return &ClientAcceptor{
		maxBuffSizeBytes:   maxBuffSizeBytes,
		clientsChan:        make(chan *follower.Client),
		server:             ss.NewTCPServer(servicePort),
		eventsQueueMaxSize: eventsQueueMaxSize,
	}
}

func (ca *ClientAcceptor) GetMsg() *follower.Client {
	select {
	case client := <-ca.clientsChan:
		return client
	default:
		return nil
	}
}

func (ca *ClientAcceptor) Start() {
	ca.server.Start(ca.handler)
}

func (ca *ClientAcceptor) Stop() {
	ca.server.Stop()
}

func serveClient(conn net.Conn, cl *follower.Client) {
	var clientReqBuff bytes.Buffer
	var clientReq *follower.Request
	for {
		select {
		case clientReq = <-cl.Chan:
			clientReqBuff.WriteString(clientReq.Payload)
			clientReqBuff.WriteRune('\n')
			_, err := conn.Write(clientReqBuff.Bytes())
			clientReqBuff.Reset()
			clientReq.Response <- err
			if err != nil {
				return
			}
		default:
			// time.Sleep(1 * time.Millisecond)
			time.Sleep(500 * time.Microsecond)
		}
	}
}

func (ca *ClientAcceptor) handler(conn net.Conn) {
	ca.mx.RLock()
	maxBuffSizeBytes := ca.maxBuffSizeBytes
	eventsQueueMaxSize := ca.eventsQueueMaxSize
	ca.mx.RUnlock()
	buff := make([]byte, maxBuffSizeBytes)
	read_len, err := conn.Read(buff)
	if err != nil {
		log.Printf("INFO: Client connection closed: %v\n", err)
		return
	}
	req := strings.Fields(string(buff[:read_len]))
	clientId, err := strconv.ParseUint(req[0], 10, 64)
	if err != nil {
		log.Printf("ERROR: adding new client: %v\n", err)
		return
	}
	cl := &follower.Client{
		ID:   clientId,
		Chan: make(chan *follower.Request, eventsQueueMaxSize),
	}
	ca.clientsChan <- cl
	log.Printf("INFO: Client `%v` connected\n", clientId)

	serveClient(conn, cl) // NOTE: blocking
}

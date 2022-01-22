package server

import (
	"bytes"
	// "log"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/gasparian/follower-maze/internal/follower"
	ss "github.com/gasparian/follower-maze/pkg/socket-server"
	"github.com/golang/glog"
)

type clientConn struct {
	ID   uint64
	conn net.Conn
	ch   chan *follower.Request
}

type ClientAcceptor struct {
	mx                 sync.RWMutex
	maxBuffSizeBytes   int
	server             ss.SocketServer
	clientsChan        chan *follower.Client
	clientsConnsChan   chan *clientConn
	clientsConns       map[uint64]*clientConn
	eventsQueueMaxSize int
}

func NewClientAcceptor(maxBuffSizeBytes, eventsQueueMaxSize int, servicePort string) *ClientAcceptor {
	return &ClientAcceptor{
		maxBuffSizeBytes:   maxBuffSizeBytes,
		clientsChan:        make(chan *follower.Client),
		clientsConnsChan:   make(chan *clientConn),
		server:             ss.NewTCPServer(servicePort),
		eventsQueueMaxSize: eventsQueueMaxSize,
		clientsConns:       make(map[uint64]*clientConn),
	}
}

func (ca *ClientAcceptor) GetNextEvent() *follower.Client {
	client, ok := <-ca.clientsChan
	if !ok {
		return nil
	}
	return client
}

func (ca *ClientAcceptor) Start() {
	go ca.serveClients()
	ca.server.Start(ca.handler)
}

func (ca *ClientAcceptor) Stop() {
	ca.server.Stop()
}

func (ca *ClientAcceptor) serveClients() {
	var clientReqBuff bytes.Buffer
	var clientReq *follower.Request
	var clientConn *clientConn
	for {
		select {
		case clientConn = <-ca.clientsConnsChan:
			ca.clientsConns[clientConn.ID] = clientConn
		default:
			clientsToRemove := make([]uint64, 0)
			for clientID, clientConn := range ca.clientsConns {
				select {
				case clientReq = <-clientConn.ch:
					clientReqBuff.WriteString(clientReq.Payload)
					clientReqBuff.WriteRune('\n')
					_, err := clientConn.conn.Write(clientReqBuff.Bytes())
					clientReqBuff.Reset()
					clientReq.Response <- err
					if err != nil {
						clientsToRemove = append(clientsToRemove, clientID)
						clientConn.conn.Close()
					}
				default:
				}
			}
			for _, clientID := range clientsToRemove {
				delete(ca.clientsConns, clientID)
			}
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
		// log.Printf("INFO: Client connection closed: %v\n", err)
		glog.Infof("INFO: Client connection closed: %v\n", err)
		return
	}
	req := strings.Fields(string(buff[:read_len]))
	clientID, err := strconv.ParseUint(req[0], 10, 64)
	if err != nil {
		// log.Printf("ERROR: adding new client: %v\n", err)
		glog.Errorf("ERROR: adding new client: %v\n", err)
		conn.Close()
		return
	}
	client := &follower.Client{
		ID:   clientID,
		Chan: make(chan *follower.Request, eventsQueueMaxSize),
	}
	ca.clientsChan <- client
	ca.clientsConnsChan <- &clientConn{
		ID:   client.ID,
		ch:   client.Chan,
		conn: conn,
	}

	// log.Printf("INFO: Client `%v` connected\n", clientID)
	glog.Infof("INFO: Client `%v` connected\n", clientID)
}

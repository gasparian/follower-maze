package server

import (
	"bytes"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gasparian/follower-maze/internal/follower"
	ss "github.com/gasparian/follower-maze/pkg/socket-server"
)

type ClientServer struct {
	MaxBuffSize int
	server      ss.SocketServer
	clientsChan chan *follower.Client
}

func NewClientServer(maxBuffSize int, servicePort string) *ClientServer {
	return &ClientServer{
		MaxBuffSize: maxBuffSize,
		clientsChan: make(chan *follower.Client),
		server:      ss.NewTCPServer(servicePort),
	}
}

func (ch *ClientServer) Push(val interface{}) {
	ch.clientsChan <- val.(*follower.Client)
}

func (ch *ClientServer) Pop() interface{} {
	select {
	case client := <-ch.clientsChan:
		return client
	default:
		return nil
	}
}

func (ch *ClientServer) Start() {
	ch.server.Start(ch.handler)
}

func (ch *ClientServer) Stop() {
	ch.server.Stop()
}

func (c *ClientServer) handler(conn net.Conn) {
	buff := make([]byte, c.MaxBuffSize)
	read_len, err := conn.Read(buff)
	if err != nil {
		log.Printf("INFO: Client connection closed: %v\n", err)
		return
	}
	//
	req := strings.Fields(string(buff[:read_len]))
	clientId, err := strconv.ParseUint(req[0], 10, 64)
	if err != nil {
		log.Printf("ERROR: adding new client: %v\n", err)
		return
	}
	cl := &follower.Client{
		ID:   clientId,
		Chan: make(chan *follower.Request),
	}
	c.Push(cl)

	log.Printf("INFO: Client `%v` connected\n", clientId)
	//

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
			// log.Println("DEBUG: >>>> WROTE: ", clientId, ", ", clientReq.Payload)
		default:
			err := ss.ConnCheck(conn)
			if err != nil {
				log.Printf("INFO: Client `%v` disconnected: %v\n", clientId, err)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

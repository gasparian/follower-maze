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
)

const (
	Follow       = 70
	Unfollow     = 85
	Broadcast    = 66
	PrivateMsg   = 80
	StatusUpdate = 83
)

type Event struct {
	Number     uint64
	MsgType    int
	FromUserID uint64
	ToUserID   uint64
}

type handler func(conn net.Conn)

type ClientsChans map[uint64]chan *Event

func handleEvent(conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(20000 * time.Millisecond))
	request := make([]byte, 128)
	var fromUserID, toUserID uint64
	defer conn.Close()
	for {
		read_len, err := conn.Read(request)

		if (err != nil) || (read_len == 0) {
			fmt.Fprint(os.Stderr, "Connection closed")
			break
		}

		req := strings.Fields(string(request[:read_len]))
		for _, r := range req {
			parsed := strings.Split(r, "|")
			number, _ := strconv.ParseUint(parsed[0], 10, 64)
			if len(parsed) > 2 {
				fromUserID, _ = strconv.ParseUint(parsed[2], 10, 64)
			}
			if len(parsed) > 3 {
				toUserID, _ = strconv.ParseUint(parsed[3], 10, 64)
			}
			log.Println(Event{
				Number:     number,
				MsgType:    int(parsed[1][0]),
				FromUserID: fromUserID,
				ToUserID:   toUserID,
			})
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
		log.Println("New client connected: ", clientId)
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
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
	log.SetOutput(os.Stdout) // log writes to stderr by default
	runtime.GOMAXPROCS(2)

	go StartTCPServer(":9099", handleClient)
	StartTCPServer(":9090", handleEvent)
}

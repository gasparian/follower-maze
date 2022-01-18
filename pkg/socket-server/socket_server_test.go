package socket_server

import (
	"net"
	"testing"
	"time"
)

const (
	timeoutMs = 500 * time.Millisecond
)

func TestTCPServer(t *testing.T) {
	outputChan := make(chan []byte)
	ss := NewTCPServer(":1123")
	go func() {
		ss.Start(
			func(conn net.Conn) {
				buff := make([]byte, 1)
				for {
					_, err := conn.Read(buff)
					if err == nil {
						outputChan <- buff
					}
					time.Sleep(200 * time.Millisecond)
				}
			},
		)
	}()
	time.Sleep(100 * time.Millisecond)

	conn, _ := net.Dial("tcp", ":1123")
	conn.Write([]byte{1})

	select {
	case <-outputChan:
	case <-time.After(timeoutMs):
		t.Error("timeout")
	}

	ss.Stop()
	conn.Close()

	err := ConnCheck(conn)
	if err == nil {
		t.Error()
	}
}

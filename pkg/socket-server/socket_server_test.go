package socket_server

import (
	"net"
	"testing"
	"time"
)

const (
	timeoutMs = 100 * time.Millisecond
)

func TestTCPServer(t *testing.T) {
	outputChan := make(chan bool)
	ss := NewTCPServer(":1123")
	go func() {
		ss.Start(
			func(conn net.Conn) {
				buff := make([]byte, 1)
				for {
					_, err := conn.Read(buff)
					if err != nil {
						return
					}
					outputChan <- true
					time.Sleep(timeoutMs)
				}
			},
		)
	}()
	time.Sleep(timeoutMs)
	defer ss.Stop()

	conn1, err := net.Dial("tcp", ":1123")
	if err != nil {
		t.Error(err)
	}
	defer conn1.Close()
	conn2, err := net.Dial("tcp", ":1123")
	if err != nil {
		t.Error(err)
	}
	defer conn2.Close()
	conns := []net.Conn{conn1, conn2}

	for _, conn := range conns {
		conn.Write([]byte{1})
		select {
		case <-outputChan:
		case <-time.After(timeoutMs):
			t.Fatal("timeout")
		}
	}

	ss.Stop()

	time.Sleep(timeoutMs * 2)
	for _, conn := range conns {
		if err = ConnCheck(conn); err == nil {
			t.Fatal("Connection should be closed")
		}
	}

	waitChan := make(chan bool)
	go func() {
		ss.Stop()
		waitChan <- true
	}()
	select {
	case <-waitChan:
	case <-time.After(timeoutMs):
		t.Fatal("timeout")
	}
}

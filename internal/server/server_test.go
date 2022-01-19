package server

import (
	"net"
	"testing"
	"time"
	// "github.com/gasparian/follower-maze/internal/event"
	"github.com/gasparian/follower-maze/internal/follower"
)

const (
	timeoutMs = 100 * time.Millisecond
)

func TestClientAcceptor(t *testing.T) {
	ca := NewClientAcceptor(8, 3, ":1125")
	go ca.Start()
	defer ca.Stop()

	time.Sleep(timeoutMs)

	conn, err := net.Dial("tcp", ":1125")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte("42\n"))

	time.Sleep(timeoutMs)

	cl := ca.GetMsg()
	if cl.ID != 42 {
		t.Error()
	}

	req := &follower.Request{
		Payload:  "666|F|60|50",
		Response: make(chan error),
	}
	cl.Chan <- req
	err = <-req.Response
	if err != nil {
		t.Fatal(err)
	}

	buff := make([]byte, 128)
	_, err = conn.Read(buff)
	if err != nil {
		t.Fatal(err)
	}
	ca.Stop()

	time.Sleep(timeoutMs)
	cl.Chan <- req
	errChan := make(chan error)
	go func() {
		_, err := conn.Read(buff)
		if err != nil {
			errChan <- err
		}
	}()

	select {
	case err = <-errChan:
	case <-time.After(timeoutMs):
		t.Fatal("timeout")
	}
}

func TestEventsBatched(t *testing.T) {

}

func TestEventsPQueue(t *testing.T) {

}

func TestFollowerServer(t *testing.T) {

}

package server

import (
	"github.com/gasparian/follower-maze/internal/event"
	"github.com/gasparian/follower-maze/internal/follower"
	"net"
	"testing"
	"time"
)

const (
	timeoutMs = 100 * time.Millisecond
)

var (
	events = getEvents()
)

func getEvents() map[string]*event.Event {
	e1, _ := event.NewEvent("666|F|24|42")
	e2, _ := event.NewEvent("766|P|24|42")
	e3, _ := event.NewEvent("866|U|24|42")
	e4, _ := event.NewEvent("966|B")
	return map[string]*event.Event{
		"Follow":     e1,
		"PrivateMsg": e2,
		"Unfollow":   e3,
		"Broadcast":  e4,
	}
}

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
		Payload:  events["Broadcast"].Raw,
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

func testEventsParser(t *testing.T, port string, ep EventsServer[*event.Event]) {
	conn, err := net.Dial("tcp", port)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write([]byte(events["PrivateMsg"].Raw + "\n"))

	time.Sleep(timeoutMs)

	eventRcvd := ep.GetMsg()
	if !event.Equal(events["PrivateMsg"], eventRcvd) {
		t.Error()
	}

	conn.Write([]byte(events["Unfollow"].Raw + "\n"))
	conn.Write([]byte(events["Follow"].Raw + "\n"))

	eventRcvd = ep.GetMsg()
	if !event.Equal(eventRcvd, events["Follow"]) {
		t.Fatal()
	}
}

func TestEventsParserBatched(t *testing.T) {
	port := ":1126"
	ep := NewEventsParserBatched(128, 3, 6, 100, port)
	go ep.Start()
	defer ep.Stop()
	time.Sleep(timeoutMs)
	testEventsParser(t, port, ep)
}

func TestEventsParserPQueue(t *testing.T) {
	port := ":1127"
	ep := NewEventsParserPQueue(128, 3, port)
	go ep.Start()
	defer ep.Stop()
	time.Sleep(timeoutMs)
	testEventsParser(t, port, ep)
}

func TestFollowerServer(t *testing.T) {
	eventsServer := NewEventsParserPQueue(
		128,
		10,
		":1128",
	)
	clientServer := NewClientAcceptor(
		16,
		10,
		":1129",
	)
	fs := NewFollowerServer(clientServer, eventsServer)
	go fs.Start()
	defer fs.Stop()

	time.Sleep(timeoutMs)

	clientConn42, err := net.Dial("tcp", ":1129")
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn42.Close()
	_, err = clientConn42.Write([]byte("42\n"))
	if err != nil {
		t.Fatal(err)
	}

	clientConn24, err := net.Dial("tcp", ":1129")
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn24.Close()
	_, err = clientConn24.Write([]byte("24\n"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(timeoutMs)

	t.Log(">>>>>>>>>>>>>>>", fs.clients)
	_, ok := fs.clients[42]
	if !ok {
		t.Fatal()
	}
	_, ok = fs.clients[24]
	if !ok {
		t.Fatal()
	}

	eventsConn, err := net.Dial("tcp", ":1128")
	if err != nil {
		t.Fatal(err)
	}
	eventsConn.Write([]byte(events["Broadcast"].Raw + "\n"))
	eventsConn.Write([]byte(events["Follow"].Raw + "\n"))
	eventsConn.Write([]byte(events["Unfollow"].Raw + "\n"))
	eventsConn.Write([]byte(events["PrivateMsg"].Raw + "\n"))

	buff := make([]byte, 128)
	_, err = clientConn42.Read(buff)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(">>>>>>>>>>>>>>>>>>>>>>>>>>>", string(buff))
}

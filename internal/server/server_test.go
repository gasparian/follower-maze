package server

import (
	"github.com/gasparian/follower-maze/internal/event"
	"github.com/gasparian/follower-maze/internal/follower"
	"net"
	"strings"
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

	cl := ca.GetNextEvent()
	if cl.ID != 42 {
		t.Errorf("Expected id to be 42, but got: %v", cl.ID)
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

	eventRcvd := ep.GetNextEvent()
	if !event.Equal(events["PrivateMsg"], eventRcvd) {
		t.Errorf("Expected: %s, but got: %s\n", events["PrivateMsg"].Raw, eventRcvd.Raw)
	}

	conn.Write([]byte(events["Unfollow"].Raw + "\n"))
	conn.Write([]byte(events["Follow"].Raw + "\n"))

	time.Sleep(timeoutMs)

	eventRcvd = ep.GetNextEvent()
	if !event.Equal(eventRcvd, events["Follow"]) {
		t.Fatalf("Expect: %s, got: %s\n", events["Follow"].Raw, eventRcvd.Raw)
	}
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

	eventsConn, err := net.Dial("tcp", ":1128")
	if err != nil {
		t.Fatal(err)
	}
	eventsConn.Write([]byte(
		events["Broadcast"].Raw + "\n" +
			events["Follow"].Raw + "\n" +
			events["Unfollow"].Raw + "\n" +
			events["PrivateMsg"].Raw + "\n",
	))

	time.Sleep(timeoutMs)

	buff := make([]byte, 128)
	read_len, err := clientConn42.Read(buff)
	if err != nil {
		t.Fatal(err)
	}
	result := strings.Fields(string(buff[:read_len]))
	if len(result) == 0 {
		t.Fatal("Expected to have at least one event back")
	}
	if result[0] != events["Follow"].Raw {
		t.Fatalf("Expected: %s, but got: %s\n", events["Follow"].Raw, result)
	}
}

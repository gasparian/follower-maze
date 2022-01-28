package socket_server

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ConnCheck returns error if the passed connection doesn't work
func ConnCheck(conn net.Conn) error {
	var sysErr error = nil
	rc, err := conn.(syscall.Conn).SyscallConn()
	if err != nil {
		return err
	}
	err = rc.Read(func(fd uintptr) bool {
		var buf []byte = []byte{0}
		n, _, err := syscall.Recvfrom(int(fd), buf, syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		switch {
		case n == 0 && err == nil:
			sysErr = io.EOF
		case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
			sysErr = nil
		default:
			sysErr = err
		}
		return true
	})
	if err != nil {
		return err
	}

	return sysErr
}

// SocketServer describes logic for SocketServer
type SocketServer interface {
	Start(func(net.Conn)) error
	Stop()
}

// TCPSocketServer holds logic for running TCP-based sockets
type TCPSocketServer struct {
	mx          sync.RWMutex
	servicePort string
	stopSignal  chan bool
	stoppedFlag int32
	connsChans  []chan bool
}

// NewTCPServer creates new instance of TCPSocketServer
func NewTCPServer(servicePort string) *TCPSocketServer {
	return &TCPSocketServer{
		stopSignal:  make(chan bool),
		servicePort: servicePort,
	}
}

func (ss *TCPSocketServer) isStopped() bool {
	stopped := atomic.LoadInt32(&ss.stoppedFlag)
	if stopped > 0 {
		return true
	}
	return false
}

func (ss *TCPSocketServer) listenStopSignal(listener *net.TCPListener) {
	<-ss.stopSignal
	listener.Close()
	atomic.StoreInt32(&ss.stoppedFlag, 1)
	wg := sync.WaitGroup{}
	ss.mx.RLock()
	for _, ch := range ss.connsChans {
		wg.Add(1)
		go func(ch chan bool) {
			defer wg.Done()
			ch <- true
		}(ch)
	}
	wg.Wait()
	ss.mx.RUnlock()
	ss.stopSignal <- true
}

func (ss *TCPSocketServer) addConnStopSignal(connStopSignal chan bool) {
	ss.mx.Lock()
	ss.connsChans = append(ss.connsChans, connStopSignal)
	ss.mx.Unlock()
}

func connListenStopSignal(conn net.Conn, connStopSignal chan bool) {
	for {
		select {
		case <-connStopSignal:
			conn.Close()
			return
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// Starts runs TCP server: accepts clients and passes connection to the
// provided handler function. Right now, handler function should
// manage the connection lifecycle (like closing it)
func (ss *TCPSocketServer) Start(h func(net.Conn)) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", ss.servicePort)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}
	go ss.listenStopSignal(listener)
	for {
		conn, err := listener.Accept()
		if ss.isStopped() {
			return nil
		}
		if err != nil {
			continue
		}
		connStopSignal := make(chan bool)
		ss.addConnStopSignal(connStopSignal)
		go func() {
			go connListenStopSignal(conn, connStopSignal)
			h(conn)
		}()
	}
}

// Stops shuts down socket server
func (ss *TCPSocketServer) Stop() {
	if !ss.isStopped() {
		ss.stopSignal <- true
		<-ss.stopSignal
	}
}

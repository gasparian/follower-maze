package socket_server

import (
	"io"
	"log"
	"net"
	"os"
	"sync/atomic"
	"syscall"
)

func checkError(err error) {
	if err != nil {
		log.Printf("ERROR: Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

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

type ConnHandler func(conn net.Conn)

type SocketServer interface {
	Start(ConnHandler)
	Stop()
}

type TCPSocketServer struct {
	address     string
	stopSignal  chan bool
	stoppedFlag int32
}

func NewTCPServer(address string) *TCPSocketServer {
	return &TCPSocketServer{
		address: address,
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
	go func() {
		<-ss.stopSignal
		listener.Close()
		atomic.StoreInt32(&ss.stoppedFlag, 1)
	}()
}

func (ss *TCPSocketServer) Start(connHandler ConnHandler) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", ss.address)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	ss.listenStopSignal(listener)
	checkError(err)
	log.Println("INFO: Starting tcp server...")
	for {
		conn, err := listener.Accept()
		if ss.isStopped() {
			return
		}
		if err != nil {
			log.Println(err)
			continue
		}
		go func() {
			defer conn.Close()
			connHandler(conn)
		}()
	}
}

func (ss *TCPSocketServer) Stop() {
	ss.stopSignal <- true
}

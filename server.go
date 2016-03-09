package grpc_gracefully_restart

import (
	log "code.google.com/p/log4go"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"net"
	"os"
	"time"
)

type Server struct {
	listener *net.TCPListener
	s        *grpc.Server
}

type RegisterFunc func(s *grpc.Server)

func newServer(caddr string) (*Server, error) {
	s := &Server{s: grpc.NewServer()}

	addr, err := net.ResolveTCPAddr("tcp", caddr)
	if err != nil {
		return nil, fmt.Errorf("fail to resolve addr: %v", err)
	}
	sock, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("fail to listen tcp: %v", err)
	}

	s.listener = sock
	return s, nil
}

func newServerFromFD(fd uintptr, savePath string) (*Server, error) {
	s := &Server{s: grpc.NewServer()}

	file := os.NewFile(fd, savePath)
	listener, err := net.FileListener(file)
	if err != nil {
		return nil, errors.New("File to recover socket from file descriptor: " + err.Error())
	}
	listenerTCP, ok := listener.(*net.TCPListener)
	if !ok {
		return nil, fmt.Errorf("File descriptor %d is not a valid TCP socket", fd)
	}
	s.listener = listenerTCP

	return s, nil
}

func (s *Server) startRpc(f RegisterFunc) {
	f(s.s)
	go s.s.Serve(s.listener)
	log.Info("start rpc on %s", s.addr())
}

func (s *Server) stopRpc() {
	s.s.Stop()
}

func (s *Server) addr() net.Addr {
	return s.listener.Addr()
}

var WaitTimeoutError = errors.New("timeout")

func (s *Server) waitWithTimeout(duration time.Duration) error {
	timeout := time.NewTimer(duration)
	wait := make(chan struct{})
	go func() {
		s.wait()
		wait <- struct{}{}
	}()

	select {
	case <-timeout.C:
		return WaitTimeoutError
	case <-wait:
		return nil
	}
}

func (s *Server) stop() {
	// Accept will instantly return a timeout error
	s.listener.SetDeadline(time.Now())
}

func (s *Server) listenerFD() (uintptr, error) {
	file, err := s.listener.File()
	if err != nil {
		return 0, err
	}
	return file.Fd(), nil
}

func (s *Server) wait() {
	time.Sleep((time.Second * 120))
}

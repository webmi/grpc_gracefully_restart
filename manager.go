package grpc_gracefully_restart

import (
	log "code.google.com/p/log4go"
	"fmt"
	"os"
	"sync"
	"time"
)

type Manager struct {
	msList []*Server
	wg     sync.WaitGroup
	closed bool
}

func NewManager() *Manager {
	return &Manager{
		msList: []*Server{},
	}
}

func (m *Manager) LoadServers(env string, addr []string) {
	if os.Getenv(env) == "true" {
		for idx, _ := range addr {
			s, err := newServerFromFD(3, fmt.Sprintf("%s_%d", addr, idx))
			if err != nil {
				log.Exitf("fail to init server:", err)
			}
			m.msList = append(m.msList, s)
		}
	} else {
		for _, addr := range addr {
			s, err := newServer(addr)
			if err != nil {
				log.Exitf("fail to init server:", err)
			}
			m.msList = append(m.msList, s)
		}
	}
}

func (m *Manager) StartRpc(f RegisterFunc) {
	for _, s := range m.msList {
		s.startRpc(f)
	}
}

func (m *Manager) StopNewConn() {
	for _, s := range m.msList {
		s.stop()
	}
}

func (m *Manager) StopRpc() {
	for _, s := range m.msList {
		s.stopRpc()
	}
	log.Info("grpc stopped")
}

func (m *Manager) WaitTimeout() {
	for _, s := range m.msList {
		go func() {
			m.wg.Add(1)
			err := s.waitWithTimeout(10 * time.Second)
			if err == WaitTimeoutError {
				log.Info("Timeout when stopping server, active connections will be cut.\n")
				os.Exit(-127)
			}
			m.wg.Done()
		}()
	}
	m.wg.Wait()
}

func (m *Manager) Wait() {
	time.Sleep(time.Second * 20)
}

func (m *Manager) Close() {
	m.closed = true
}

func (m *Manager) IsClose() bool {
	return m.closed
}

func (m *Manager) ListenerFds() (fds []uintptr) {
	fds = make([]uintptr, len(m.msList))
	var err error
	for i, s := range m.msList {
		if fds[i], err = s.listenerFD(); err != nil {
			log.Exitf("s.ListenerFD() error(%v)", err)
		}
	}
	return
}

package server

import (
	"crypto/tls"
	"errors"
	"net"
	"nrRPC/log"
	"runtime"
	"sync"
	"time"
)

// ErrServerClosed is returned by the Server's Serve,ListenAndServe after a call to shut down or Close
var ErrServerClosed = errors.New("http: Server closed")

const (
	// DefaultRPCPath is used by ServeHTTP
	DefaultRPCPath = "/_rpcx_"

	// ReaderBuffsize is used for bufio reader
	ReaderBuffsize = 16 * 1024

	// WriterBuffsize if used for bufio writer
	WriterBuffsize = 16 * 1024
)

// contextKey is a value for use with context.WithValue.It's used as
// a pointer so it fits in an interface{} without allocation
type contextKey struct {
	name string
}

func (k *contextKey) String() string {
	return "rpc context value " + k.name
}

var (
	// RemoteConnContextKey is a context key.It can be used in
	// services with context.WithValue to access the connection arrived at.
	// The associated value will be of type net.Conn
	RemoteConnContextKey = &contextKey{"remote-conn"}
)

// Server is rpc server that use TCP or UDP.
type Server struct {
	ln           net.Listener
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	mu           sync.Mutex
	activeConn   map[net.Conn]struct{}
	doneChan     chan struct{}
}

func (s *Server) getDoneChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.doneChan == nil {
		s.doneChan = make(chan struct{})
	}
	return s.doneChan
}

func (s *Server) idleTimeout() time.Duration {
	if s.IdleTimeout != 0 {
		return s.IdleTimeout
	}
	return s.ReadTimeout
}

// Server accepts incoming connections on the Listener ln,
// creating a new service goroutine for each.
// The service goroutines read requests and then call services to reply to them.
func (s *Server) Serve(ln net.Listener) error {
	s.ln = ln

	var tempDelay time.Duration

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.getDoneChan():
				return ErrServerClosed
			default:
			}

			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}

				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}

				log.Errorf("rpcx: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0

		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetKeepAlive(true)
			tc.SetKeepAlivePeriod(3 * time.Minute)
		}

		s.mu.Lock()
		s.activeConn[conn] = struct{}{}
		s.mu.Unlock()

		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.Errorf("serving %s panic error: %s,stack:\n %s", conn.RemoteAddr(), err, buf)
		}
		s.mu.Lock()
		delete(s.activeConn, conn)
		s.mu.Unlock()
		conn.Close()
	}()

	if tlsConn, ok := conn.(*tls.Conn); ok {
		if d := s.ReadTimeout; d != 0 {
			conn.SetReadDeadline(time.Now().Add(d))
		}
		if d := s.WriteTimeout; d != 0 {
			conn.SetWriteDeadline(time.Now().Add(d))
		}
		if err := tlsConn.Handshake(); err != nil {
			log.Errorf("rpc: TLS handshake error from %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

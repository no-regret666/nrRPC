package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"nrRPC/log"
	"nrRPC/protocol"
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

	ctx := context.WithValue(context.Background(), RemoteConnContextKey, conn)
	r := bufio.NewReaderSize(conn, ReaderBuffsize)
	w := bufio.NewWriterSize(conn, WriterBuffsize)

	for {
		if s.IdleTimeout != 0 {
			conn.SetReadDeadline(time.Now().Add(s.IdleTimeout))
		}

		t0 := time.Now()
		if s.ReadTimeout != 0 {
			conn.SetReadDeadline(t0.Add(s.ReadTimeout))
		}

		if s.WriteTimeout != 0 {
			conn.SetWriteDeadline(t0.Add(s.WriteTimeout))
		}

		req, err := s.readRequest(ctx, r)
		if err != nil {
			log.Errorf("rpc: failed to read request: %v", err)
			return
		}

		resp, err := s.handleRequest(ctx, req)
		if err != nil {
			log.Error("rpc:failed to handle request: %v", err)
		}

		resp.WriteTo(w)
	}
}

func (s *Server) readRequest(ctx context.Context, r io.Reader) (req *protocol.Message, err error) {
	req, err = protocol.Read(r)
	return req, err
}

func (s *Server) handleRequest(ctx context.Context, req *protocol.Message) (resp *protocol.Message, err error) {

}

// Can connect to RPC service using HTTP CONNECT to rpcPath
var connected = "200 Connected to Go RPC"

// ServeHTTP implements an http.Handler that answers RPC requests
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Info("rpc hijacking ", req.RemoteAddr, ":", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	s.serveConn(conn)
}

// HandleHTTP registers an HTTP handler for RPC messages on rpcPath,
// and a debugging handler on debugPath.
// It is still necessary to invoke http.Serve(),typically in a go statement.
func (s *Server) HandleHTTP(rpcPath, debugPath string) {
	http.Handle(rpcPath, s)
}

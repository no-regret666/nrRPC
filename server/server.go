package server

import (
	"bufio"
	"context"
	"crypto/tls"
	_ "debug/macho"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"nrRPC/log"
	"nrRPC/protocol"
	"reflect"
	"runtime"
	"sync"
	"time"
)

// ErrServerClosed is returned by the Server's Serve,ListenAndServe after a call to shut down or Close
var ErrServerClosed = errors.New("http: Server closed")

const (
	// DefaultRPCPath is used by ServeHTTP
	DefaultRPCPath = "/_rpc_"

	// ReaderBuffsize is used for bufio reader
	ReaderBuffsize = 16 * 1024

	// WriterBuffsize if used for bufio writer
	WriterBuffsize = 16 * 1024

	// ServicePath is service name
	ServicePath = "_rpc_path_"

	//ServiceMethod is name of the service
	ServiceMethod = "_rpc_method_"

	// ServiceError contains error info of service invocation
	ServiceError = "_rpc_error_"
)

var (
	codecs = map[protocol.SerializeType]codec.Codec{
		protocol.SerializeNone: &codec.ByteCodec{},
		protocol.JSON:          &codec.JSONCodec{},
	}
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
	// 网络连接相关
	ln           net.Listener  // 监听套接字
	ReadTimeout  time.Duration // 读取请求超时时间
	WriteTimeout time.Duration // 写入响应超时时间
	IdleTimeout  time.Duration // 空闲连接超时时间

	// 服务注册相关（并发安全）
	ServiceMapMu sync.RWMutex
	serviceMap   map[string]*service
	funcMap      map[string]*methodType

	// 连接管理相关（并发安全）
	mu         sync.RWMutex          //保护 activeConn 的读写锁
	activeConn map[net.Conn]struct{} // 活跃连接集合
	doneChan   chan struct{}         // 服务器关闭信号通道
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

				log.Errorf("rpc: Accept error: %v; retrying in %v", err, tempDelay)
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

		go func() {
			resp, err := s.handleRequest(ctx, req)
			if err != nil {
				log.Error("rpc:failed to handle request: %v", err)
			}
			resp.WriteTo(w)
		}()
	}
}

func (s *Server) readRequest(ctx context.Context, r io.Reader) (req *protocol.Message, err error) {
	req, err = protocol.Read(r)
	return req, err
}

func (s *Server) handleRequest(ctx context.Context, req *protocol.Message) (resp *protocol.Message, err error) {
	resp = req.Clone()
	resp.SetMessageType(protocol.Response)

	serviceName := req.Metadata[ServicePath]
	methodName := req.Metadata[ServiceMethod]

	s.ServiceMapMu.RLock()
	service := s.serviceMap[serviceName]
	s.ServiceMapMu.RUnlock()
	if service == nil {
		err = errors.New("rpc: can't find service " + serviceName)
		resp.SetMessageStatusType(protocol.Error)
		resp.Metadata[ServiceError] = err.Error()
		return
	}
	mtype := service.method[methodName]
	if mtype == nil {
		err = errors.New("rpc: can't find method " + methodName)
		resp.SetMessageStatusType(protocol.Error)
		resp.Metadata[ServiceError] = err.Error()
		return
	}

	var argv, replyv reflect.Value

	argsIsValue := false // if true,need to indirect before calling
	if mtype.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(mtype.ArgType.Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argsIsValue = true
	}

	if argsIsValue {
		argv = argv.Elem()
	}

	codec := codecs[req.SerializeType()]
	if codec == nil {
		err = fmt.Errorf("can not find codec for %d", req.SerializeType())
		resp.SetMessageStatusType(protocol.Error)
		resp.Metadata[ServiceError] = err.Error()
		return
	}

	err = codec.Decode(req.Payload, argv.Interface())
	if err != nil {
		resp.SetMessageStatusType(protocol.Error)
		resp.Metadata[ServiceError] = err.Error()
		return
	}

	replyv = reflect.New(mtype.ReplyType.Elem())

	err = service.call(ctx, mtype, argv, replyv)
	if err != nil {
		resp.SetMessageStatusType(protocol.Error)
		resp.Metadata[ServiceError] = err.Error()
		return resp, err
	}

	data, err := codec.Encode(replyv.Interface())
	if err != nil {
		resp.SetMessageStatusType(protocol.Error)
		resp.Metadata[ServiceError] = err.Error()
		return resp, err
	}
	resp.Payload = data
	return resp, nil
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

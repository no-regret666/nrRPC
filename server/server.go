package server

import (
	"bufio"
	"context"
	"crypto/tls"
	_ "debug/macho"
	"errors"
	"fmt"
	"github.com/xtaci/kcp-go"
	"io"
	"net"
	"net/http"
	"nrRPC/log"
	"nrRPC/protocol"
	"nrRPC/share"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

// ErrServerClosed is returned by the Server's Serve,ListenAndServe after a call to shut down or Close
var ErrServerClosed = errors.New("http: Server closed")

const (
	ReaderBuffsize = 1024
	WriterBuffsize = 1024
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

	// 服务注册相关（并发安全）
	ServiceMapMu sync.RWMutex
	serviceMap   map[string]*service

	// 连接管理相关（并发安全）
	mu         sync.RWMutex          //保护 activeConn 的读写锁
	activeConn map[net.Conn]struct{} // 活跃连接集合
	doneChan   chan struct{}         // 服务器关闭信号通道

	inShutdown int32
	onShutdown []func()

	KCPConfig  KCPConfig
	QUICConfig QUICConfig
}

type KCPConfig struct {
	BlockCrypt kcp.BlockCrypt
}

type QUICConfig struct {
	TlsConfig *tls.Config
}

func (s *Server) Address() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) getDoneChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.doneChan == nil {
		s.doneChan = make(chan struct{})
	}
	return s.doneChan
}

func (s *Server) Serve(network, address string) (err error) {
	var ln net.Listener
	ln, err = s.makeListener(network, address)
	if err != nil {
		return
	}

	if network == "http" {
		s.serveByHTTP(ln, "")
		return nil
	}
	return s.ServeListener(ln)
}

func (s *Server) ServeListener(ln net.Listener) error {
	s.ln = ln

	var tempDelay time.Duration

	s.mu.Lock()
	if s.activeConn == nil {
		s.activeConn = make(map[net.Conn]struct{})
	}
	s.mu.Unlock()

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

func (s *Server) serveByHTTP(ln net.Listener, rpcPath string) {
	s.ln = ln

	if rpcPath == "" {
		rpcPath = share.DefaultRPCPath
	}
	http.Handle(rpcPath, s)
	srv := &http.Server{Handler: nil}

	s.mu.Lock()
	if s.activeConn == nil {
		s.activeConn = make(map[net.Conn]struct{})
	}
	s.mu.Unlock()

	srv.Serve(ln)
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
		t0 := time.Now()
		if s.ReadTimeout != 0 {
			conn.SetReadDeadline(t0.Add(s.ReadTimeout))
		}

		req, err := s.readRequest(ctx, r)
		if err != nil {
			log.Errorf("rpc: failed to read request: %v", err)
			return
		}

		if s.WriteTimeout != 0 {
			conn.SetWriteDeadline(t0.Add(s.WriteTimeout))
		}

		go func() {
			resp, err := s.handleRequest(ctx, req)
			if err != nil {
				log.Errorf("rpc:failed to handle request: %v", err)
			}
			resp.WriteTo(w)
			w.Flush()
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

	serviceName := req.Metadata[protocol.ServicePath]
	methodName := req.Metadata[protocol.ServiceMethod]

	s.ServiceMapMu.RLock()
	service := s.serviceMap[serviceName]
	s.ServiceMapMu.RUnlock()
	if service == nil {
		err = errors.New("rpc: can't find service " + serviceName)
		return handleError(resp, err)
	}
	mtype := service.method[methodName]
	if mtype == nil {
		err = errors.New("rpc: can't find method " + methodName)
		return handleError(resp, err)
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

	codec := share.Codecs[req.SerializeType()]
	if codec == nil {
		err = fmt.Errorf("can not find codec for %d", req.SerializeType())
		return handleError(resp, err)
	}

	err = codec.Decode(req.Payload, argv.Interface())
	if err != nil {
		return handleError(resp, err)
	}

	replyv = reflect.New(mtype.ReplyType.Elem())

	err = service.call(ctx, mtype, argv, replyv)
	if err != nil {
		return handleError(resp, err)
	}

	data, err := codec.Encode(replyv.Interface())
	if err != nil {
		return handleError(resp, err)
	}
	resp.Payload = data
	return resp, nil
}

func handleError(res *protocol.Message, err error) (*protocol.Message, error) {
	res.SetMessageStatusType(protocol.Error)
	res.Metadata[protocol.ServiceError] = err.Error()
	return res, err
}

// Can connect to RPC service using HTTP CONNECT to rpcPath
var connected = "200 Connected to rpc"

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

	s.mu.Lock()
	s.activeConn[conn] = struct{}{}
	s.mu.Unlock()

	s.serveConn(conn)
}

func (s *Server) HandleHTTP(rpcPath, debugPath string) {
	http.Handle(rpcPath, s)
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeDoneChanLocked()
	err := s.ln.Close()

	for c := range s.activeConn {
		c.Close()
		delete(s.activeConn, c)
	}
	return err
}

func (s *Server) RegisterOnShutdown(f func()) {
	s.mu.Lock()
	s.onShutdown = append(s.onShutdown, f)
	s.mu.Unlock()
}

func (s *Server) closeDoneChanLocked() {
	ch := s.getDoneChanLocked()
	select {
	case <-ch:
	// 已经关闭
	default:
		close(ch)
	}
}

func (s *Server) getDoneChanLocked() chan struct{} {
	if s.doneChan == nil {
		s.doneChan = make(chan struct{})
	}
	return s.doneChan
}

var ip4Reg = regexp.MustCompile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`)

func ValidIP4(ipAddress string) bool {
	ipAddress = strings.Trim(ipAddress, " ")
	i := strings.LastIndex(ipAddress, ":")
	ipAddress = ipAddress[:i]

	return ip4Reg.MatchString(ipAddress)
}

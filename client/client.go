package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"nrRPC/log"
	"nrRPC/protocol"
	"nrRPC/share"
	"nrRPC/util"
	"sync"
	"time"

	circuit "github.com/rubyist/circuitbreaker"
	kcp "github.com/xtaci/kcp-go"
)

var (
	ErrShutdown         = errors.New("connection is shutdown")
	ErrUnsupportedCodec = errors.New("unsupported codec")
)

var CircuitBreaker = circuit.NewRateBreaker(0.95, 100)

const (
	ReaderBuffsize = 16 * 1024
	WriterBuffsize = 16 * 1024
)

type seqKey struct{}

type Client struct {
	TLSConfig *tls.Config
	Block     kcp.BlockCrypt

	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration

	UseCircuitBreaker bool

	Conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	SerializeType protocol.SerializeType
	CompressType  protocol.CompressType

	mutex    sync.Mutex
	seq      uint64
	pending  map[uint64]*Call
	closing  bool //用户关闭
	shutdown bool // 服务端关闭
}

type Call struct {
	ServicePath   string
	ServiceMethod string
	Metadata      map[string]string
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

func (call *Call) done() {
	select {
	case call.Done <- call:
	// ok
	default:
		log.Debug("rpc: discarding Call reply due to insufficient Done chan capacity")
	}
}

// Go : 异步调用
func (client *Client) Go(ctx context.Context, servicePath, serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	call := new(Call)
	call.ServicePath = servicePath
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply
	if done == nil {
		done = make(chan *Call, 10)
	} else {
		// 禁止使用无缓冲通道，可能导致死锁
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	client.send(ctx, call)
	return call
}

// Call : 同步调用
func (client *Client) Call(ctx context.Context, servicedPath, serviceMethod string, args interface{}, reply interface{}) error {
	if client.UseCircuitBreaker {
		return CircuitBreaker.Call(func() error {
			return client.call(ctx, servicedPath, serviceMethod, args, reply)
		}, 0)
	}

	return client.call(ctx, servicedPath, serviceMethod, args, reply)
}

func (client *Client) call(ctx context.Context, servicePath, serviceMethod string, args interface{}, reply interface{}) error {
	seq := new(uint64)
	ctx = context.WithValue(ctx, seqKey{}, seq)
	Done := client.Go(ctx, servicePath, serviceMethod, args, reply, make(chan *Call, 1)).Done

	var err error
	//阻塞等待
	select {
	case <-ctx.Done(): // cancel by context
		client.mutex.Lock()
		call := client.pending[*seq]
		delete(client.pending, *seq)
		client.mutex.Unlock()
		if call != nil {
			call.Error = ctx.Err()
			call.done()
		}

		return ctx.Err()
	case call := <-Done: // 调用完成
		err = call.Error
	}

	return err
}

func (client *Client) send(ctx context.Context, call *Call) {
	client.mutex.Lock()
	if client.shutdown || client.closing {
		call.Error = ErrShutdown
		client.mutex.Unlock()
		call.done()
		return
	}

	codec := share.Codecs[client.SerializeType]
	if codec == nil {
		call.Error = ErrUnsupportedCodec
		client.mutex.Unlock()
		call.done()
		return
	}

	if client.pending == nil {
		client.pending = make(map[uint64]*Call)
	}

	seq := client.seq
	client.seq++
	client.pending[seq] = call
	client.mutex.Unlock()

	req := protocol.NewMessage()
	req.SetMessageType(protocol.Request)
	req.SetSeq(seq)
	req.SetSerializeType(client.SerializeType)
	req.Metadata[protocol.ServicePath] = call.ServicePath
	req.Metadata[protocol.ServiceMethod] = call.ServiceMethod

	data, err := codec.Encode(call.Args)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	if len(data) > 1024 && client.CompressType == protocol.Gzip {
		data, err = util.Zip(data)
		if err != nil {
			call.Error = err
			call.done()
			return
		}

		req.SetCompressType(client.CompressType)
	}

	req.Payload = data

	err = req.WriteTo(client.w)
	if err == nil {
		err = client.w.Flush()
	}
	if err != nil {
		client.mutex.Lock()
		call = client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (client *Client) input() {
	var err error
	var res *protocol.Message
	for err == nil {
		res, err = protocol.Read(client.r)
		if err != nil {
			break
		}

		seq := res.Seq()
		client.mutex.Lock()
		call := client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()

		switch {
		case call == nil:
			// TODO
		case res.MessageStatusType() == protocol.Error:
			call.Error = errors.New(res.Metadata[protocol.ServiceError])
			call.done()
		default:
			data := res.Payload
			if res.CompressType() == protocol.Gzip {
				data, err = util.Unzip(data)
				if err != nil {
					call.Error = errors.New("unzip payload: " + err.Error())
				}
			}

			codec := share.Codecs[res.SerializeType()]
			if codec == nil {
				call.Error = ErrUnsupportedCodec
			} else {
				err = codec.Decode(data, call.Reply)
				if err != nil {
					call.Error = err
				}
			}

			call.done()
		}
	}

	client.mutex.Lock()
	client.shutdown = true
	closing := client.closing
	if err == io.EOF {
		if closing {
			err = ErrShutdown
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
	client.mutex.Unlock()
	if err != io.EOF && !closing {
		log.Error("rpc: client protocol error:", err)
	}
}

func (client *Client) Close() error {
	client.mutex.Lock()

	for seq, call := range client.pending {
		delete(client.pending, seq)
		if call != nil {
			call.Error = ErrShutdown
			call.done()
		}
	}

	if client.closing || client.shutdown {
		client.mutex.Unlock()
		return ErrShutdown
	}

	client.closing = true
	client.mutex.Unlock()
	return client.Conn.Close()
}

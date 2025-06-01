package client

import (
	"crypto/tls"
	quicconn "github.com/marten-seemann/quic-conn"
	"github.com/xtaci/kcp-go"
	"net"
)

func newDirectKCPConn(c *Client, network, address string, opts ...interface{}) (net.Conn, error) {
	var conn net.Conn
	var err error

	conn, err = kcp.DialWithOptions(address, c.Block, 10, 3)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func newDirectQuicConn(c *Client, network, address string, opts ...interface{}) (net.Conn, error) {
	var conn net.Conn
	var err error

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	conn, err = quicconn.Dial(address, tlsConfig)

	if err != nil {
		return nil, err
	}
	return conn, nil
}

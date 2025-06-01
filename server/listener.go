package server

import (
	quicconn "github.com/marten-seemann/quic-conn"
	"github.com/xtaci/kcp-go"
	"net"

	reuseport "github.com/kavu/go_reuseport"
)

func (s *Server) makeListener(network, address string) (ln net.Listener, err error) {
	switch network {
	case "reuseport":
		if ValidIP4(address) {
			network = "tcp4"
		} else {
			network = "tcp6"
		}

		ln, err = reuseport.NewReusablePortListener(network, address)
	case "kcp":
		ln, err = kcp.ListenWithOptions(address, s.KCPConfig.BlockCrypt, 10, 3)
	case "quic":
		ln, err = quicconn.Listen("udp", address, s.QUICConfig.TlsConfig)
	default: //tcp,http
		ln, err = net.Listen(network, address)
	}

	return ln, err
}

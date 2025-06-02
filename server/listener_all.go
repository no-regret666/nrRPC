package server

import (
	"errors"
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
		if s.Options == nil || s.Options["BlockCrypt"] == nil {
			return nil, errors.New("KCP BlockCrypt must be configured in server.Options")
		}
		ln, err = kcp.ListenWithOptions(address, s.Options["BlockCrypt"].(kcp.BlockCrypt), 10, 3)
	default: //tcp,http
		ln, err = net.Listen(network, address)
	}

	return ln, err
}

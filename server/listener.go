package server

import (
	"net"

	reuseport "github.com/kavu/go_reuseport"
)

func makeListener(network, address string) (ln net.Listener, err error) {
	switch network {
	case "reuseport":
		if ValidIP4(address) {
			network = "tcp4"
		} else {
			network = "tcp6"
		}

		ln, err = reuseport.NewReusablePortListener(network, address)

	default:
		ln, err = net.Listen(network, address)
	}

	return ln, err
}

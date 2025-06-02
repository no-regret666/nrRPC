package serverplugin

import (
	"github.com/rcrowley/go-metrics"
	"nrRPC/server"
	"testing"
	"time"
)

func TestEtcdRegistry(t *testing.T) {
	s := &server.Server{}

	r := &EtcdRegistryPlugin{
		ServiceAddress: "tcp@127.0.0.1:8972",
		EtcdServers:    []string{"127.0.0.1:2379"},
		BasePath:       "/rpc_test",
		Metrics:        metrics.NewRegistry(),
		Services:       make([]string, 1),
		UpdateInterval: time.Minute,
	}
	err := r.Start()
	if err != nil {
		t.Fatal(err)
	}
	s.Plugins.Add(r)

	s.RegisterName("Arith", new(Arith), "")
	go s.Serve("tcp", "127.0.0.1:8972")
	defer s.Close()

	if len(r.Services) != 1 {
		t.Fatal("failed to register service in etcd")
	}
}

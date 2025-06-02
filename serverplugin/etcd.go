package serverplugin

import (
	"errors"
	"fmt"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/etcd"
	metrics "github.com/rcrowley/go-metrics"
	"net"
	"net/url"
	"nrRPC/log"
	"strconv"
	"strings"
	"time"
)

type EtcdRegistryPlugin struct {
	ServiceAddress string
	EtcdServers    []string
	BasePath       string
	Metrics        metrics.Registry
	Services       []string
	UpdateInterval time.Duration

	kv store.Store
}

func (p *EtcdRegistryPlugin) Start() error {
	etcd.Register()
	kv, err := libkv.NewStore(store.ETCD, p.EtcdServers, nil)
	if err != nil {
		log.Errorf("cannot create etcd registry: %v", err)
		return err
	}
	p.kv = kv

	if p.BasePath[0] == '/' {
		p.BasePath = p.BasePath[1:]
	}

	err = kv.Put(p.BasePath, []byte("rpc_path"), &store.WriteOptions{IsDir: true})
	if err != nil && !strings.Contains(err.Error(), "Not a file") {
		log.Errorf("cannot create etcd path %s: %v", p.BasePath, err)
		return err
	}

	if p.UpdateInterval > 0 {
		ticker := time.NewTicker(p.UpdateInterval)
		go func() {
			defer p.kv.Close()

			for range ticker.C {
				clientMeter := metrics.GetOrRegisterMeter("clientMeter", p.Metrics)
				data := []byte(strconv.FormatInt(clientMeter.Count()/60, 10))
				for _, name := range p.Services {
					nodePath := fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
					kvPaire, err := p.kv.Get(nodePath)
					if err != nil {
						log.Infof("can't get data of node: %s,because of %v", nodePath, err.Error())
					} else {
						v, _ := url.ParseQuery(string(kvPaire.Value))
						v.Set("tps", string(data))
						p.kv.Put(nodePath, []byte(v.Encode()), &store.WriteOptions{TTL: p.UpdateInterval * 2})
					}
				}
			}
		}()
	}
	return nil
}

func (p *EtcdRegistryPlugin) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	if p.Metrics != nil {
		clientMeter := metrics.GetOrRegisterMeter("clientMeter", p.Metrics)
		clientMeter.Mark(1)
	}
	return conn, true
}

func (p *EtcdRegistryPlugin) Register(name string, rcvr interface{}, metadata ...string) (err error) {
	if "" == strings.TrimSpace(name) {
		err = errors.New("register service name can't be empty")
		return
	}

	if p.kv == nil {
		etcd.Register()
		kv, err := libkv.NewStore(store.ETCD, p.EtcdServers, nil)
		if err != nil {
			log.Errorf("cannot create etcd registry: %v", err)
			return err
		}
		p.kv = kv
	}

	if p.BasePath[0] == '/' {
		p.BasePath = p.BasePath[1:]
	}
	err = p.kv.Put(p.BasePath, []byte("rpc_path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create etcd path %s: %v", p.BasePath, err)
		return err
	}

	nodePath := fmt.Sprintf("%s/%s", p.BasePath, name)
	err = p.kv.Put(nodePath, []byte(name), &store.WriteOptions{IsDir: true})
	if err != nil && !strings.Contains(err.Error(), "Not a file") {
		log.Errorf("cannot create etcd path %s: %v", nodePath, err)
		return err
	}

	nodePath = fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
	err = p.kv.Put(nodePath, []byte(p.ServiceAddress), &store.WriteOptions{TTL: p.UpdateInterval * 2})
	if err != nil {
		log.Errorf("cannot create etcd path %s: %v", nodePath, err)
		return err
	}

	p.Services = append(p.Services, name)
	return
}

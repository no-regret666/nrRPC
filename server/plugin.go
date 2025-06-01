package server

import (
	"context"
	"net"
	"nrRPC/errors"
	"nrRPC/protocol"
)

type PluginContainer interface {
	Add(plugin Plugin)
	Remove(plugin Plugin)
	All() []Plugin

	DoRegister(name string, rcvr interface{}, metadata string) error

	DoPostConnAccept(net.Conn) (net.Conn, bool)

	DoPreReadRequest(ctx context.Context) error
	DoPostReadRequest(ctx context.Context, r *protocol.Message, e error) error

	DoPreWriteResponse(context.Context, *protocol.Message) error
	DoPostWriteReponse(context.Context, *protocol.Message, *protocol.Message, error) error
}

type Plugin interface{}

type (
	RegisterPlugin interface {
		Register(name string, rcvr interface{}, metadata string) error
	}

	PostConnAcceptPlugin interface {
		HandleConnAccept(net.Conn) (net.Conn, bool)
	}

	PreReadRequestPlugin interface {
		PreReadRequest(ctx context.Context) error
	}

	PostReadRequestPlugin interface {
		PostReadRequest(ctx context.Context, req *protocol.Message, e error) error
	}

	PreWriteResponsePlugin interface {
		PreWriteResponse(context.Context, *protocol.Message) error
	}

	PostWriteResponsePlugin interface {
		PostWriteResponse(context.Context, *protocol.Message, *protocol.Message, error) error
	}
)

type pluginContainer struct {
	plugins []Plugin
}

func (p *pluginContainer) Add(plugin Plugin) {
	p.plugins = append(p.plugins, plugin)
}

func (p *pluginContainer) Remove(plugin Plugin) {
	if p.plugins == nil {
		return
	}

	var plugins []Plugin
	for _, p := range p.plugins {
		if p != plugin {
			plugins = append(plugins, p)
		}
	}

	p.plugins = plugins
}

func (p *pluginContainer) All() []Plugin {
	return p.plugins
}

func (p *pluginContainer) DoRegister(name string, rcvr interface{}, metadata string) error {
	var es []error
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(RegisterPlugin); ok {
			err := plugin.Register(name, rcvr, metadata)
			if err != nil {
				es = append(es, err)
			}
		}
	}

	if len(es) > 0 {
		return errors.NewMultiError(es)
	}
	return nil
}

func (p *pluginContainer) DoPostConnAccept(conn net.Conn) (net.Conn, bool) {
	var flag bool
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PostConnAcceptPlugin); ok {
			conn, flag = plugin.HandleConnAccept(conn)
			if !flag { // interrupt
				conn.Close()
				return conn, false
			}
		}
	}
	return conn, true
}

func (p *pluginContainer) DoPreReadRequest(ctx context.Context) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PreReadRequestPlugin); ok {
			err := plugin.PreReadRequest(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *pluginContainer) DoPostReadRequest(ctx context.Context, r *protocol.Message, e error) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PostReadRequestPlugin); ok {
			err := plugin.PostReadRequest(ctx, r, e)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *pluginContainer) DoPreWriteResponse(ctx context.Context, r *protocol.Message) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PreWriteResponsePlugin); ok {
			err := plugin.PreWriteResponse(ctx, r)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *pluginContainer) DoPostWriteResponse(ctx context.Context, r *protocol.Message, resp *protocol.Message, e error) error {
	for i := range p.plugins {
		if plugin, ok := p.plugins[i].(PostWriteResponsePlugin); ok {
			err := plugin.PostWriteResponse(ctx, r, resp, e)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

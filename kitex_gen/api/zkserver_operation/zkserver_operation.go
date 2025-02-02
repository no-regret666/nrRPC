// Code generated by Kitex v0.12.1. DO NOT EDIT.

package zkserver_operation

import (
	"context"
	"errors"
	client "github.com/cloudwego/kitex/client"
	kitex "github.com/cloudwego/kitex/pkg/serviceinfo"
	operations "nrMQ/kitex_gen/api"
)

var errInvalidMessageType = errors.New("invalid message type for service method handler")

var serviceMethods = map[string]kitex.MethodInfo{
	"CreateTopic": kitex.NewMethodInfo(
		createTopicHandler,
		newZkServer_OperationCreateTopicArgs,
		newZkServer_OperationCreateTopicResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
}

var (
	zkServer_OperationServiceInfo                = NewServiceInfo()
	zkServer_OperationServiceInfoForClient       = NewServiceInfoForClient()
	zkServer_OperationServiceInfoForStreamClient = NewServiceInfoForStreamClient()
)

// for server
func serviceInfo() *kitex.ServiceInfo {
	return zkServer_OperationServiceInfo
}

// for stream client
func serviceInfoForStreamClient() *kitex.ServiceInfo {
	return zkServer_OperationServiceInfoForStreamClient
}

// for client
func serviceInfoForClient() *kitex.ServiceInfo {
	return zkServer_OperationServiceInfoForClient
}

// NewServiceInfo creates a new ServiceInfo containing all methods
func NewServiceInfo() *kitex.ServiceInfo {
	return newServiceInfo(false, true, true)
}

// NewServiceInfo creates a new ServiceInfo containing non-streaming methods
func NewServiceInfoForClient() *kitex.ServiceInfo {
	return newServiceInfo(false, false, true)
}
func NewServiceInfoForStreamClient() *kitex.ServiceInfo {
	return newServiceInfo(true, true, false)
}

func newServiceInfo(hasStreaming bool, keepStreamingMethods bool, keepNonStreamingMethods bool) *kitex.ServiceInfo {
	serviceName := "ZkServer_Operation"
	handlerType := (*operations.ZkServer_Operation)(nil)
	methods := map[string]kitex.MethodInfo{}
	for name, m := range serviceMethods {
		if m.IsStreaming() && !keepStreamingMethods {
			continue
		}
		if !m.IsStreaming() && !keepNonStreamingMethods {
			continue
		}
		methods[name] = m
	}
	extra := map[string]interface{}{
		"PackageName": "api",
	}
	if hasStreaming {
		extra["streaming"] = hasStreaming
	}
	svcInfo := &kitex.ServiceInfo{
		ServiceName:     serviceName,
		HandlerType:     handlerType,
		Methods:         methods,
		PayloadCodec:    kitex.Thrift,
		KiteXGenVersion: "v0.12.1",
		Extra:           extra,
	}
	return svcInfo
}

func createTopicHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*operations.ZkServer_OperationCreateTopicArgs)
	realResult := result.(*operations.ZkServer_OperationCreateTopicResult)
	success, err := handler.(operations.ZkServer_Operation).CreateTopic(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newZkServer_OperationCreateTopicArgs() interface{} {
	return operations.NewZkServer_OperationCreateTopicArgs()
}

func newZkServer_OperationCreateTopicResult() interface{} {
	return operations.NewZkServer_OperationCreateTopicResult()
}

type kClient struct {
	c client.Client
}

func newServiceClient(c client.Client) *kClient {
	return &kClient{
		c: c,
	}
}

func (p *kClient) CreateTopic(ctx context.Context, req *operations.CreateTopicRequest) (r *operations.CreateTopicResponse, err error) {
	var _args operations.ZkServer_OperationCreateTopicArgs
	_args.Req = req
	var _result operations.ZkServer_OperationCreateTopicResult
	if err = p.c.Call(ctx, "CreateTopic", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

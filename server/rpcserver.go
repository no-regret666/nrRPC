package server

import (
	"context"
	"fmt"
	"github.com/cloudwego/kitex/server"
	"io"
	"nrMQ/kitex_gen/api"
	"nrMQ/kitex_gen/api/server_operations"
	"nrMQ/kitex_gen/api/zkserver_operations"
	"nrMQ/logger"
	"nrMQ/zookeeper"
)

// RPCServer 可以注册两种server
// 一个是客户端（生产者和消费者连接的模式，负责订阅等请求和对zookeeper的请求）使用的server
// 另一个是Broker获取zookeeper信息和调度各个Broker的调度者
type RPCServer struct {
	srv_cli  *server.Server
	srv_bro  *server.Server
	zkinfo   zookeeper.ZkInfo
	server   *Server
	zkserver *ZKServer
}

func NewRPCServer(zkinfo zookeeper.ZkInfo) RPCServer {
	logger.LOGinit()
	return RPCServer{
		zkinfo: zkinfo,
	}
}

func (s *RPCServer) Start(opts_cli, opts_zks, opts_raf []server.Option, opt Options) error {
	switch opt.Tag {
	case BROKER:
		s.server = NewServer(s.zkinfo)
		s.server.Make(opt, opts_raf)

		srv_cli_bro := server_operations.NewServer(s, opts_cli...)
		s.srv_cli = &srv_cli_bro
		logger.DEBUG(logger.DLog, "%v the raft %v start rpcserver for clients\n", opt.Name, opt.Me)
		go func() {
			err := srv_cli_bro.Run()

			if err != nil {
				fmt.Println(err.Error())
			}
		}()

	case ZKBROKER:
		s.zkserver = NewZKServer(s.zkinfo)
		s.zkserver.make(opt)

		srv_bro_cli := zkserver_operations.NewServer(s, opts_zks...)
		s.srv_bro = &srv_bro_cli
		logger.DEBUG(logger.DLog, "ZkServer start rpcserver for brokers\n")
		go func() {
			err := srv_bro_cli.Run()

			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}

	return nil
}

func (s *RPCServer) ShutDown_server() {
	if s.srv_bro != nil {
		(*s.srv_bro).Stop()
	}
	if s.zkserver != nil {
		(*s.srv_cli).Stop()
	}
}

// producer--->zkserver
// 先在zookeeper上创建一个Topic,当生产该信息时，或消费信息时再有zkserver发送信息到broker让broker创建
func (s *RPCServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (r *api.CreateTopicResponse, err error) {
	info := s.zkserver.CreateTopic(Info_in{
		topicName: req.TopicName,
	})

	if info.Err != nil {
		return &api.CreateTopicResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.CreateTopicResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// producer--->zkserver
// 先在zookeeper上创建一个Partition,当生产该信息时，或消费信息时再有zkserver发送信息到broker让broker创建
func (s *RPCServer) CreatePart(ctx context.Context, req *api.CreatePartRequest) (r *api.CreatePartResponse, err error) {
	info := s.zkserver.CreatePart(Info_in{
		topicName: req.TopicName,
		partName:  req.PartName,
	})

	if info.Err != nil {
		return &api.CreatePartResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.CreatePartResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// producer---->zkserver 获取该向哪个broker发送消息
func (s *RPCServer) ProGetBroker(ctx context.Context, req *api.ProGetBrokRequest) (r *api.ProGetBrokResponse, err error) {
	info := s.zkserver.ProGetBroker(Info_in{
		topicName: req.TopicName,
		partName:  req.PartName,
	})

	if info.Err != nil {
		return &api.ProGetBrokResponse{
			Ret: false,
		}, info.Err
	}

	return &api.ProGetBrokResponse{
		Ret:            true,
		BrokerHostPort: info.broHostPort,
	}, nil
}

// producer---->broker server
func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (r *api.PushResponse, err error) {
	ret, err := s.server.PushHandle(info{
		producer:  req.Producer,
		topicName: req.Topic,
		partName:  req.Key,
		ack:       req.Ack,
		message:   req.Message,
	})

	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	return &api.PushResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// consumer---->broker server
// 获取broker的ip和port
func (s *RPCServer) ConInfo(ctx context.Context, req *api.InfoRequest) (r *api.InfoResponse, err error) {
	err = s.server.InfoHandle(req.IpPort)
	if err == nil {
		return &api.InfoResponse{
			Ret: true,
		}, nil
	}
	return &api.InfoResponse{Ret: false}, err
}

// consumer---->broker server
func (s *RPCServer) StartToGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetResponse, err error) {
	err = s.server.StartGet(info{
		consumer:   req.CliName,
		topic_name: req.TopicName,
		part_name:  req.PartName,
		offset:     req.Offset,
	})

	if err == nil {
		return &api.InfoGetResponse{
			Ret: true,
		}, nil
	}

	return &api.InfoGetResponse{Ret: false}, err
}

// consumer---->broker server
func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (r *api.PullResponse, err error) {
	Err := "ok"
	ret, err := s.server.PullHandle(info{
		consumer:   req.Consumer,
		topic_name: req.Topic,
		part_name:  req.Key,
		size:       req.Size,
		offset:     req.Offset,
		option:     req.Option,
	})
	if err != nil {
		if err == io.EOF && ret.size == 0 {
			Err = "file EOF"
		} else {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return &api.PullResponse{
				Ret: false,
				Err: err.Error(),
			}, err
		}
	}

	return &api.PullResponse{
		Msgs: ret.array,
	}
}

func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (r *api.SubResponse, err error) {
	err = s.zkserver.SubHandle(Info_in{
		cli_name:   req.Consumer,
		topic_name: req.Topic,
		part_name:  req.Key,
		option:     req.Option,
	})

	if err == nil {
		return &api.SubResponse{
			Ret: true,
		}, nil
	}

	return &api.SubResponse{Ret: false}, err
}

func (s *RPCServer) ConStartGetBroker(ctx context.Context, req *api.ConStartGetBrokRequest) (r *api.ConStartGetBrokResponse, err error) {
	parts, size, err := s.zkserver.HandStartGetBroker(Info_in{
		cli_name:   req.CliName,
		topic_name: req.TopicName,
		part_name:  req.PartName,
		option:     req.Option,
		index:      req.Index,
	})
	if err != nil {
		return &api.ConStartGetBrokResponse{
			Ret: false,
		}, err
	}

	return &api.ConStartGetBrokResponse{
		Ret:   true,
		Size:  int64(size),
		Parts: parts,
	}, nil
}

// zkserver---->broker server
// 通知broker准备接收生产者信息
func (s *RPCServer) PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest) (r *api.PrepareAcceptResponse, err error) {
	ret, err := s.server.PrepareAcceptHandle(info{
		topic_name: req.TopicName,
		part_name:  req.PartName,
		file_name:  req.FileName,
	})
	if err != nil {
		return &api.PrepareAcceptResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.PrepareAcceptResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// zkserver---->broker server
// zkserver控制broker停止接收某个partition的信息
// 并修改文件名，关闭partition的fd等(指NowBlock的接收信息)
// 调用者需向zookeeper修改节点信息
func (s *RPCServer) CloseAccept(ctx context.Context, req *api.CloseAcceptRequest) (r *api.CloseAcceptResponse, err error) {
	start, end, ret, err := s.server.CloseAcceptHandle(info{
		topic_name: req.TopicName,
		part_name:  req.PartName,
		file_name:  req.Oldfilename,
		new_name:   req.Newfilename_,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "Err %v err(%v)\n", ret, err.Error())
		return &api.CloseAcceptResponse{
			Ret: false,
		}, err
	}

	return &api.CloseAcceptResponse{
		Ret:        true,
		Startindex: start,
		Endindex:   end,
	}, nil
}

func (s *RPCServer) PrepareState(ctx context.Context, req *api.PrepareStateRequest) (r *api.PrepareStateResponse, err error) {
	//TODO implement me
	panic("implement me")
}

// zkserver---->broker server
// 通知broker准备向consumer发送信息
func (s *RPCServer) PrepareSend(ctx context.Context, req *api.PrepareSendRequest) (r *api.PrepareSendResponse, err error) {
	ret, err := s.server.PrepareSendHandle(info{
		consumer:   req.Consumer,
		topic_name: req.TopicName,
		part_name:  req.PartName,
		option:     req.Option,
		file_name:  req.FileName,
		offset:     req.Offset,
	})
	if err != nil {
		return &api.PrepareSendResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.PrepareSendResponse{
		Ret: true,
		Err: ret,
	}, nil
}

// broker---->zkserver
// broker连接到zkserver后会立即发送info,让zkserver连接到broker
func (s *RPCServer) BroInfo(ctx context.Context, req *api.BroInfoRequest) (r *api.BroInfoResponse, err error) {
	err = s.zkserver.HandleBroInfo(req.BrokerName, req.BrokerHostPort)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return &api.BroInfoResponse{
			Ret: false,
		}, err
	}
	return &api.BroInfoResponse{
		Ret: true,
	}, nil
}

func (s *RPCServer) UpdateDup(ctx context.Context, req *api.UpdateDupRequest) (r *api.UpdateDupResponse, err error) {
	//TODO implement me
	panic("implement me")
}

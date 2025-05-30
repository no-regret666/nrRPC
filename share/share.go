package share

import (
	"nrRPC/codec"
	"nrRPC/protocol"
)

const (
	DefaultRPCPath = "/_rpc_"
)

var (
	Codecs = map[protocol.SerializeType]codec.Codec{
		protocol.SerializeNone: &codec.ByteCodec{},
		protocol.JSON:          &codec.JSONCodec{},
		protocol.ProtoBuffer:   &codec.PBCodec{},
		protocol.MsgPack:       &codec.MsgpackCodec{},
	}
)

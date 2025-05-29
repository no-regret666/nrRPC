package codec

import (
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	proto "google.golang.org/protobuf/proto"
)

type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode(data []byte, i interface{}) error
}

type ByteCodec struct{}

func (b ByteCodec) Encode(i interface{}) ([]byte, error) {
	if data, ok := i.([]byte); ok {
		return data, nil
	}

	// %T 获取变量的具体类型
	return nil, fmt.Errorf("%T is not []byte", i)
}

func (b ByteCodec) Decode(data []byte, i interface{}) error {
	i = &data
	return nil
}

type JSONCodec struct{}

func (c JSONCodec) Encode(i interface{}) ([]byte, error) {
	return json.Marshal(i)
}

func (c JSONCodec) Decode(data []byte, i interface{}) error {
	return json.Unmarshal(data, i)
}

type PBCodec struct{}

func (c PBCodec) Encode(i interface{}) ([]byte, error) {
	if m, ok := i.(proto.Message); ok {
		return proto.Marshal(m)
	}

	return nil, fmt.Errorf("%T does not implement proto.Message interface", i)
}

func (c PBCodec) Decode(data []byte, i interface{}) error {
	if msg, ok := i.(proto.Message); ok {
		return proto.Unmarshal(data, msg)
	}
	return fmt.Errorf("%T dose not implement proto.Message interface", i)
}

type MsgpackCodec struct{}

func (c MsgpackCodec) Encode(i interface{}) ([]byte, error) {
	return msgpack.Marshal(i)
}

func (c MsgpackCodec) Decode(data []byte, i interface{}) error {
	return msgpack.Unmarshal(data, i)
}

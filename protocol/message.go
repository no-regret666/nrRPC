package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"nrRPC/util"
)

const (
	magicNumber byte = 0x88
)

var (
	lineSeparator = []byte("\r\n")
)

var (
	// ErrMetaKVMissing some keys or values are missing
	ErrMetaKVMissing = errors.New("wrong metadata lines. some keys or values are missing")
)

type MessageType byte

const (
	// itoa：预定义的常量标识符，在const块中生成一组按 自增规律 排列的常量，避免手动为每个常量赋值
	Request MessageType = iota
	Response
)

// MessageStatusType is status of messages
type MessageStatusType byte

const (
	// Normal : requests / responses
	Normal MessageStatusType = iota
	// Error indicates some errors occur
	Error
)

// CompressType : 定义解压缩类型
type CompressType byte

const (
	None CompressType = iota
	Gzip
)

type SerializeType byte

const (
	Serialize SerializeType = iota
	JSON
	ProtoBuffer
	MsgPack
)

// Message : Request / Response
type Message struct {
	*Header
	Metadata map[string]string
	Payload  []byte
}

func NewMessage() *Message {
	header := Header([12]byte{})
	header[0] = magicNumber
	return &Message{
		Header:   &header,
		Metadata: make(map[string]string),
	}
}

// Header :
// +------------------------------------------------------------+
// | magicNumber | version | 标志位（7-0位） | serialize | seq... |
// +---------------------------------------------------- -------+
// h[2]第7位为消息类型，第6位为心跳标志，第5位为单项消息标志
// 第4-2位为压缩类型，第1-0位为消息状态类型
type Header [12]byte

func (h Header) CheckMagicNumber() bool {
	return h[0] == magicNumber
}

func (h Header) Version() byte {
	return h[1]
}

func (h *Header) SetVersion(v byte) {
	h[1] = v
}

func (h Header) MessageType() MessageType {
	return MessageType(h[2] & 0x80)
}

func (h *Header) SetMessageType(mt MessageType) {
	h[2] = h[2] | (byte(mt) << 7)
}

// IsHeartbeat 是否是心跳消息
func (h Header) IsHeartbeat() bool {
	return h[2]&0x40 == 0x40
}

func (h *Header) SetHeartbeat(hb bool) {
	if hb {
		h[2] = h[2] | 0x40
	} else {
		h[2] = h[2] &^ 0x40
	}
}

// IsOneWay 返回消息是否为单向消息，如果为 true，则 server 不会发送响应。
func (h Header) IsOneWay() bool {
	return h[2]&0x20 == 0x20
}

func (h *Header) SetOneWay(oneway bool) {
	if oneway {
		h[2] = h[2] | 0x40
	} else {
		h[2] = h[2] &^ 0x40
	}
}

func (h Header) CompressType() CompressType {
	return CompressType((h[2] & 0x1C) >> 2)
}

func (h *Header) SetCompressType(ct CompressType) {
	h[2] = h[2] | ((byte(ct) << 2) & 0x1C)
}

func (h Header) MessageStatusType() MessageStatusType {
	return MessageStatusType((h[2] & 0x03))
}

func (h *Header) SetMessageStatusType(mt MessageStatusType) {
	h[2] = h[2] | (byte(mt) & 0x03)
}

func (h Header) SerializeType() SerializeType {
	return SerializeType((h[3] & 0xF0) >> 4)
}

func (h *Header) SetSerializeType(st SerializeType) {
	h[3] = h[3] | (byte(st) << 4)
}

func (h Header) Seq() uint64 {
	return binary.BigEndian.Uint64(h[4:])
}

func (h *Header) SetSeq(seq uint64) {
	binary.BigEndian.PutUint64(h[4:], seq)
}

func (m Message) Clone() *Message {
	header := *m.Header
	return &Message{
		Header:   &header,
		Metadata: make(map[string]string),
	}
}

func (m Message) Encode() []byte {
	meta := encodeMetadata(m.Metadata)

	l := 12 + (4 + len(meta) + (4 + len(m.Payload)))

	data := make([]byte, l)
	copy(data, m.Header[:])
	binary.BigEndian.PutUint32(data[12:16], uint32(len(meta)))
	copy(data[12:], meta)
	binary.BigEndian.PutUint32(data[16+len(meta):], uint32(len(m.Payload)))
	copy(data[20+len(meta):], m.Payload)

	return data
}

func (m *Message) WriteTo(w io.Writer) error {
	_, err := w.Write(m.Header[:])
	if err != nil {
		return err
	}
	meta := encodeMetadata(m.Metadata)
	err = binary.Write(w, binary.BigEndian, uint32(len(meta)))
	if err != nil {
		return err
	}

	_, err = w.Write(meta)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, uint32(len(m.Payload)))
	if err != nil {
		return err
	}

	_, err = w.Write(m.Payload)
	return err
}

func encodeMetadata(m map[string]string) []byte {
	var buf bytes.Buffer
	for k, v := range m {
		buf.WriteString(k)
		buf.Write(lineSeparator)
		buf.WriteString(v)
		buf.Write(lineSeparator)
	}

	return buf.Bytes()
}

func decodeMetadata(lenData []byte, r io.Reader) (map[string]string, error) {
	_, err := io.ReadFull(r, lenData)
	if err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenData)
	m := make(map[string]string)
	if l == 0 {
		return m, nil
	}

	data := make([]byte, l)
	_, err = io.ReadFull(r, data)
	if err != nil {
		return nil, err
	}

	meta := bytes.Split(data, lineSeparator)

	// last element is empty
	if len(meta)%2 != 1 {
		return nil, ErrMetaKVMissing
	}
	for i := 0; i < len(meta)-1; i += 2 {
		m[util.SliceByteToString(meta[i])] = string(meta[i+1])
	}
	return m, nil
}

func Read(r io.Reader) (*Message, error) {
	msg := NewMessage()
	_, err := io.ReadFull(r, msg.Header[:])
	if err != nil {
		return nil, err
	}

	lenData := make([]byte, 4)
	msg.Metadata, err = decodeMetadata(lenData, r)
	if err != nil {
		return nil, err
	}

	_, err = io.ReadFull(r, lenData)
	if err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenData)

	msg.Payload = make([]byte, l)
	_, err = io.ReadFull(r, msg.Payload)

	return msg, err
}

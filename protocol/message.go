package protocol

import "errors"

const (
	magicNumber byte = 0x88
)

var (
	lineSeparator = []byte("\r\n")
)

var (
	ErrMetaKVMissing = errors.New("wrong metadata lines. some keys or values are missing")
)

// Message is message type of requests and responses.
type MessageType byte

const (
	Request MessageType = iota
	Response
)

// MessageStatusType is status of messages
type MessageStatusType byte

const (
	// Normal is normal requests and responses
	Normal MessageStatusType = iota
	Error
)

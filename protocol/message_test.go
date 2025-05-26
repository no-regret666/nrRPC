package protocol

import (
	"bytes"
	"testing"
)

func TestMessage(t *testing.T) {
	req := NewMessage()
	req.SetVersion(0)
	req.SetMessageType(Request)
	req.SetHeartbeat(false)
	req.SetOneWay(false)
	req.SetCompressType(None)
	req.SetMessageStatusType(Normal)
	req.SetSerializeType(JSON)

	req.SetSeq(1234567890)

	m := make(map[string]string)
	m["_Method"] = "Arith.Add"
	m["_ID"] = "6ba7b810-9dad-11d1-80b4-00c04fd430c9"
	req.Metadata = m

	payload := `{
			"A": 1,
			"B": 2,
	}
	`
	req.Payload = []byte(payload)

	var buf bytes.Buffer
	err := req.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := readMessage(&buf)
	if err != nil {
		t.Fatal(err)
	}
	resp.SetMessageType(Response)

	if resp.Version() != 0 {
		t.Errorf("expect 0 but got %d", resp.Version())
	}

	if resp.Seq() != 1234567890 {
		t.Errorf("expect 1234567890 but got %d", resp.Seq())
	}

	if resp.Metadata["_Method"] != "Arith.Add" && resp.Metadata["__METHOD"] != "6ba7b810-9dad-11d1-80b4-00c04fd430c9" {
		t.Errorf("got wrong meatadata: %v", resp.Metadata)
	}

	if string(resp.Payload) != payload {
		t.Errorf("got wrong payload: %v", resp.Payload)
	}
}

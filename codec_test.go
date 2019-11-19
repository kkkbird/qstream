package qstream

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sirupsen/logrus"
)

var (
	log = logrus.WithField("pkg", "qstream")
)

type SimpleData struct {
	ID      int
	Message string
}

func testCodec(t *testing.T, codec DataCodec) {
	var (
		d = SimpleData{
			ID:      1234,
			Message: "Hello",
		}
	)

	m, err := codec.Encode(&d)
	assert.NoError(t, err, "encode error")

	log.Info(m)

	d2, err := codec.Decode(m)
	assert.NoError(t, err, "decode error")

	assert.Equal(t, *d2.(*SimpleData), d)
}

func benchmarkCodec(b *testing.B, codec DataCodec) {
	var (
		d = SimpleData{
			ID:      1234,
			Message: "Hello",
		}
	)
	for i := 0; i < b.N; i++ {
		d.ID = i
		m, _ := codec.Encode(&d)
		codec.Decode(m)
	}
}

func TestStructCodec(t *testing.T) {
	testCodec(t, StructCodec(SimpleData{}))
}

func BenchmarkStructCodec(b *testing.B) {
	benchmarkCodec(b, StructCodec(SimpleData{}))
}

func TestJsonCodec(t *testing.T) {
	testCodec(t, JsonCodec(SimpleData{}))
}

func BenchmarkJsonCodec(b *testing.B) {
	benchmarkCodec(b, JsonCodec(SimpleData{}))
}

func TestMsgpackCodec(t *testing.T) {
	testCodec(t, MsgpackCodec(SimpleData{}))
}

func BenchmarkMsgpackCodec(b *testing.B) {
	benchmarkCodec(b, MsgpackCodec(SimpleData{}))
}

type customizeCodec struct {
}

func (c customizeCodec) Encode(d interface{}) (map[string]interface{}, error) {
	d2 := d.(*SimpleData)
	return map[string]interface{}{
		"ID":      strconv.Itoa(d2.ID),
		"Message": d2.Message,
	}, nil
}

func (c customizeCodec) Decode(v map[string]interface{}) (interface{}, error) {
	id, _ := strconv.Atoi(v["ID"].(string))
	return &SimpleData{
		ID:      id,
		Message: v["Message"].(string),
	}, nil
}

var CustomizeCodec customizeCodec

func TestCustomizeCodec(t *testing.T) {
	testCodec(t, CustomizeCodec)
}

func BenchmarkCustimizeCodec(b *testing.B) {
	benchmarkCodec(b, CustomizeCodec)
}

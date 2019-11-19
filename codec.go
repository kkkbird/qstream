package qstream

import (
	"encoding/json"
	"errors"

	"reflect"

	"github.com/fatih/structs"
	"github.com/mitchellh/mapstructure"
	"github.com/vmihailenco/msgpack"
)

var (
	ErrDataDecodeFail = errors.New("ErrDataDecodeFail")
)

type DataCodec interface {
	Encode(interface{}) (map[string]interface{}, error)
	Decode(map[string]interface{}) (interface{}, error)
}

type structCodec struct {
	typ reflect.Type
}

func (c *structCodec) Encode(d interface{}) (map[string]interface{}, error) {
	return structs.Map(d), nil
}

func (c *structCodec) Decode(v map[string]interface{}) (interface{}, error) {
	d := reflect.New(c.typ)

	if err := mapstructure.WeakDecode(v, d.Interface()); err != nil {
		return nil, err
	}

	return d.Interface(), nil
}

func StructCodec(d interface{}) DataCodec {
	return &structCodec{
		typ: reflect.Indirect(reflect.ValueOf(d)).Type(),
	}
}

type jsonCodec struct {
	typ reflect.Type
}

func (c jsonCodec) Encode(d interface{}) (map[string]interface{}, error) {
	d2, err := json.Marshal(d)

	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"d": string(d2),
	}, nil
}

func (c jsonCodec) Decode(v map[string]interface{}) (interface{}, error) {
	d2, ok := v["d"]
	if !ok {
		return nil, ErrDataDecodeFail
	}

	d := reflect.New(c.typ)
	if err := json.Unmarshal([]byte(d2.(string)), d.Interface()); err != nil {
		return nil, err
	}
	return d.Interface(), nil
}

func JsonCodec(d interface{}) DataCodec {
	return &jsonCodec{
		typ: reflect.Indirect(reflect.ValueOf(d)).Type(),
	}
}

type msgpackCodec struct {
	typ reflect.Type
}

func (c msgpackCodec) Encode(d interface{}) (map[string]interface{}, error) {
	d2, err := msgpack.Marshal(d)

	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"d": string(d2),
	}, nil
}

func (c msgpackCodec) Decode(v map[string]interface{}) (interface{}, error) {
	d2, ok := v["d"]
	if !ok {
		return nil, ErrDataDecodeFail
	}

	d := reflect.New(c.typ)
	if err := msgpack.Unmarshal([]byte(d2.(string)), d.Interface()); err != nil {
		return nil, err
	}
	return d.Interface(), nil
}

func MsgpackCodec(d interface{}) DataCodec {
	return &msgpackCodec{
		typ: reflect.Indirect(reflect.ValueOf(d)).Type(),
	}
}

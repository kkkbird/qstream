package qstream

import (
	"encoding/json"
	"errors"
	"fmt"

	"reflect"

	"github.com/fatih/structs"
	"github.com/mitchellh/mapstructure"
	"github.com/vmihailenco/msgpack"
)

var (
	ErrDataDecodeFail  = errors.New("ErrDataDecodeFail")
	ErrTypeNotRegister = errors.New("Type not register")
	ErrTypeNameMissing = errors.New("Missing type name")
)

type typeRegistry map[string]reflect.Type

func (tr *typeRegistry) Get(d interface{}) (reflect.Type, string) {
	tname := fmt.Sprintf("%T", reflect.Indirect(reflect.ValueOf(d)).Interface())

	return tr.GetByName(tname), tname
}

func (tr *typeRegistry) GetByName(tname string) reflect.Type {

	if typ, ok := (*tr)[tname]; ok {
		return typ
	}

	return nil
}

func newTypeRegistry(d []interface{}) typeRegistry {
	if len(d) == 0 {
		panic("must have a type")
	}

	r := make(map[string]reflect.Type)
	for _, dtyp := range d {
		v := reflect.Indirect(reflect.ValueOf(dtyp))
		r[fmt.Sprintf("%T", v.Interface())] = v.Type()
	}

	return typeRegistry(r)
}

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
	tr typeRegistry
}

func (c jsonCodec) Encode(d interface{}) (map[string]interface{}, error) {
	typ, tname := c.tr.Get(d)

	if typ == nil {
		return nil, ErrTypeNotRegister
	}

	d2, err := json.Marshal(d)

	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"___typ": tname,
		"d":      string(d2),
	}, nil
}

func (c jsonCodec) Decode(v map[string]interface{}) (interface{}, error) {
	tname, ok := v["___typ"]
	if !ok {
		return nil, ErrTypeNameMissing
	}

	d2, ok := v["d"]
	if !ok {
		return nil, ErrDataDecodeFail
	}

	typ := c.tr.GetByName(tname.(string))

	if typ == nil {
		return nil, ErrTypeNotRegister
	}

	d := reflect.New(typ)
	if err := json.Unmarshal([]byte(d2.(string)), d.Interface()); err != nil {
		return nil, err
	}
	return d.Interface(), nil
}

func JsonCodec(d ...interface{}) DataCodec {
	return &jsonCodec{
		tr: newTypeRegistry(d),
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

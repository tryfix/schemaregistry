/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package schemaregistry

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/hamba/avro/v2"

	"github.com/linkedin/goavro"
	"github.com/tryfix/errors"
)

// Encoder holds the reference to Registry and Subject which can be used to encode and decode messages
type Encoder struct {
	subject  *Subject
	registry *Registry
	codec    *goavro.Codec
	api      avro.API
}

// NewEncoder return the pointer to a Encoder for given Subject from the Registry
func NewEncoder(reg *Registry, subject *Subject) (*Encoder, error) {
	codec, err := goavro.NewCodec(subject.Schema)
	if err != nil {
		reg.logger.Error(fmt.Sprintf(`cannot init encoder due to codec failed due to %+v`, err))
		return nil, errors.WithPrevious(err, fmt.Sprintf(`cannot init encoder due to codec failed due to %+v`, err))
	}

	return &Encoder{
		subject:  subject,
		registry: reg,
		codec:    codec,
		api:      avro.DefaultConfig,
	}, nil
}

// Encode return a byte slice with a avro encoded message. magic byte and schema id will be appended to its beginning
//
//	╔════════════════════╤════════════════════╤══════════════════════╗
//	║ magic byte(1 byte) │ schema id(4 bytes) │ AVRO encoded message ║
//	╚════════════════════╧════════════════════╧══════════════════════╝
func (s *Encoder) Encode(data interface{}) ([]byte, error) {
	return encodeHamba(s.subject.Id, s.codec.Schema(), data)
}

// Decode returns the decoded go interface of avro encoded message and error if its unable to decode
func (s *Encoder) Decode(data []byte) (interface{}, error) {
	return decodeHamba(s.registry.idMap, data)
}

func encodePrefix(id int) []byte {
	byt := make([]byte, 5)
	binary.BigEndian.PutUint32(byt[1:], uint32(id))
	return byt
}

func decodePrefix(byt []byte) int {
	return int(binary.BigEndian.Uint32(byt[1:5]))
}

// Schema return the subject asociated with the Encoder
func (s *Encoder) Schema() string {
	return s.subject.Schema
}

func encode(subjectId int, codec *goavro.Codec, data interface{}) ([]byte, error) {
	byt, err := json.Marshal(data)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`json marshal failed for schema [%d]`, subjectId))
	}

	native, _, err := codec.NativeFromTextual(byt)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`native from textual failed for schema [%d]`, subjectId))
	}

	magic := encodePrefix(subjectId)

	bin, err := codec.BinaryFromNative(magic, native)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`binary from native failed for schema [%d]`, subjectId))
	}

	return bin, nil
}

func encodeHamba(subjectId int, schema string, data interface{}) ([]byte, error) {
	sch, err := avro.Parse(schema)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`parse error for schema [%d]`, subjectId))
	}
	native, err := avro.Marshal(sch, data)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`native from textual failed for schema [%d]`, subjectId))
	}
	return append(encodePrefix(subjectId), native...), nil
}

// decode returns the decoded go interface of avro encoded message and error if its unable to decode
func decode(encoders map[int]*Encoder, data []byte) (interface{}, error) {
	if len(data) < 5 {
		return nil, errors.New(`message length is zero`)
	}

	schemaID := decodePrefix(data)

	encoder, ok := encoders[schemaID]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`schema id [%d] dose not registred`, schemaID))
	}

	native, _, err := encoder.codec.NativeFromBinary(data[5:])
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`native from binary for schema id [%d] failed`, schemaID))
	}

	byt, err := encoder.codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`textual from native for schema id [%d] failed`, schemaID))
	}

	if encoder.subject.JsonDecoder == nil {
		return nil, errors.New(fmt.Sprintf(`json decoder does not exist for schema id %d`, schemaID))
	}

	return encoder.subject.JsonDecoder(byt)
}

// decode returns the decoded go interface of avro encoded message and error if its unable to decode
func decodeHamba(encoders map[int]*Encoder, data []byte) (interface{}, error) {
	if len(data) < 5 {
		return nil, errors.New(`message length is zero`)
	}

	schemaID := decodePrefix(data)

	encoder, ok := encoders[schemaID]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`schema id [%d] dose not registred`, schemaID))
	}

	//native, _, err := encoder.codec.NativeFromBinary(data[5:])
	schema, err := avro.Parse(encoder.Schema())
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`native from binary for schema id [%d] failed`, schemaID))
	}
	var v interface{}
	err = encoder.api.Unmarshal(schema, data[5:], &v)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`data unmarshal error, schema : [%d] failed`, schemaID))
	}
	return v, nil
}

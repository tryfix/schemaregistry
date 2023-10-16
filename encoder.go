/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package schemaregistry

import (
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"

	"github.com/tryfix/errors"
)

// Encoder holds the reference to Registry and Subject which can be used to encode and decode messages
type Encoder struct {
	subject  *Subject
	registry *Registry
	api      avro.API
}

type Unmarshaler interface {
	Unmarshal(in interface{}) error
}

// NewEncoder return the Encoder for given Subject from the Registry
func NewEncoder(reg *Registry, subject *Subject) *Encoder {
	return &Encoder{
		subject:  subject,
		registry: reg,
		api:      avro.DefaultConfig,
	}
}

// Encode return a byte slice with a avro encoded message. magic byte and schema id will be appended to its beginning
//
//	╔════════════════════╤════════════════════╤══════════════════════╗
//	║ magic byte(1 byte) │ schema id(4 bytes) │ AVRO encoded message ║
//	╚════════════════════╧════════════════════╧══════════════════════╝
func (s *Encoder) Encode(data interface{}) ([]byte, error) {
	return encode(s.subject.Id, s.Schema(), data)
}

// Decode returns the decoded go interface of avro encoded message and error if its unable to decode
func (s *Encoder) Decode(data []byte) (interface{}, error) {
	if len(data) < 5 {
		return nil, errors.New(`message length is zero`)
	}

	schemaID := int(binary.BigEndian.Uint32((data)[1:5]))

	encoder, ok := s.registry.idMap[schemaID]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`schema id [%d] dose not registred`, schemaID))
	}

	encoded := &AvroEncoded{
		enc:  encoder,
		data: data[5:],
	}

	return encoder.subject.UnmarshalerFunc(encoded)
}

// Schema return the subject associated with the Encoder
func (s *Encoder) Schema() string {
	return s.subject.Schema
}

type AvroEncoded struct {
	enc  *Encoder
	data []byte
}

func (s *AvroEncoded) Unmarshal(in interface{}) error {
	schema, err := avro.Parse(s.enc.Schema())
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`schema parsing error for schema %s`, s.enc.Schema()))
	}
	return s.enc.api.Unmarshal(schema, s.data, in)
}

func encodePrefix(id int) []byte {
	byt := make([]byte, 5)
	binary.BigEndian.PutUint32(byt[1:], uint32(id))
	return byt
}

func encode(subjectId int, schema string, data interface{}) ([]byte, error) {
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

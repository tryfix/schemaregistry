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

	"github.com/tryfix/errors"
)

type Encoder interface {
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte) (interface{}, error)
}

type Marshaller interface {
	Init() error
	Marshall(v interface{}) ([]byte, error)
	NewUnmarshaler(data []byte) Unmarshaler
}

type Unmarshaler interface {
	Unmarshal(in interface{}) error
}

// RegistryEncoder holds the reference to Registry and Subject which can be used to encode and decode messages
type RegistryEncoder struct {
	subject  *Subject
	registry *Registry
}

// NewRegistryEncoder NewEncoder return the Encoder for given Subject from the Registry
func NewRegistryEncoder(registry *Registry, subject *Subject) Encoder {
	return &RegistryEncoder{
		subject:  subject,
		registry: registry,
	}
}

func encodePrefix(id int) []byte {
	byt := make([]byte, 5)
	binary.BigEndian.PutUint32(byt[1:], uint32(id))
	return byt
}

// Encode return a byte slice with a avro encoded message. magic byte and schema id will be appended to its beginning
//
//	╔════════════════════╤════════════════════╤════════════════════════╗
//	║ magic byte(1 byte) │ schema id(4 bytes) │ Encoded Message		   ║
//	╚════════════════════╧════════════════════╧════════════════════════╝
func (s *RegistryEncoder) Encode(data interface{}) ([]byte, error) {
	encoded, err := s.subject.marsheller.Marshall(data)
	if err != nil {
		return nil, err
	}

	return append(encodePrefix(s.subject.Id), encoded...), nil
}

// Decode returns the decoded go interface of avro encoded message and error if its unable to decode
func (s *RegistryEncoder) Decode(data []byte) (interface{}, error) {
	if len(data) < 5 {
		return nil, errors.New(`message length is zero`)
	}

	schemaID := int(binary.BigEndian.Uint32((data)[1:5]))

GetSubject:
	subject, ok := s.registry.getSubjectBySchemaID(schemaID)
	if !ok {
		s.registry.logger.Warn(
			fmt.Sprintf(`Schema id [%d] dose not registred. Fetching from Schema registry`, schemaID))
		if err := s.registry.updateRegistryCache(schemaID); err != nil {
			s.registry.logger.Error(
				fmt.Sprintf(`Registry update failed for schema ID [%d] due to %s`, schemaID, err))
		} else {
			goto GetSubject
		}

		return nil, errors.New(fmt.Sprintf(`schema id [%d] dose not registred`, schemaID))
	}

	return subject.UnmarshalerFunc(subject.marsheller.NewUnmarshaler(data[5:]))
}

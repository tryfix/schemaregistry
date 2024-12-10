/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package schemaregistry

import (
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/tryfix/errors"
)

type AvroUnmarshaler struct {
	schema avro.Schema
	data   []byte
}

type AvroMarshaller struct {
	schema     string
	avroSchema avro.Schema
}

func NewAvroMarshaller(schema string) *AvroMarshaller {
	return &AvroMarshaller{
		schema: schema,
	}
}

func (s *AvroMarshaller) Init() error {
	schema, err := avro.Parse(s.schema)
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`schema parsing error for subject %s`, s.schema))
	}

	s.avroSchema = schema
	return nil
}

func (s *AvroMarshaller) NewUnmarshaler(data []byte) Unmarshaler {
	return &AvroUnmarshaler{
		schema: s.avroSchema,
		data:   data,
	}
}

func (s *AvroUnmarshaler) Unmarshal(in interface{}) error {
	return avro.Unmarshal(s.schema, s.data, in)
}

func (s *AvroMarshaller) Marshall(data interface{}) ([]byte, error) {
	native, err := avro.Marshal(s.avroSchema, data)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`native from textual failed for subject %s`, s.schema))
	}

	return native, nil
}

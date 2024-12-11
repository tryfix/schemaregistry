/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package schemaregistry

import (
	"github.com/tryfix/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type ProtoUnmarshaler struct {
	data []byte
}

type ProtoMarshaller struct{}

func NewProtoMarshaller() Marshaller {
	return &ProtoMarshaller{}
}

func (s *ProtoMarshaller) Init() error {
	return nil
}

func (s *ProtoMarshaller) NewUnmarshaler(data []byte) Unmarshaler {
	return &ProtoUnmarshaler{
		data: data,
	}
}

func (s *ProtoUnmarshaler) Unmarshal(in interface{}) error {
	wrapper := &anypb.Any{}
	if err := proto.Unmarshal(s.data, wrapper); err != nil {
		return errors.WithPrevious(err, "failed to unmarshal anypb wrapper")
	}

	if err := anypb.UnmarshalTo(wrapper, in.(proto.Message), proto.UnmarshalOptions{}); err != nil {
		return errors.WithPrevious(err, "failed to unmarshal anypb")
	}

	return nil
}

func (s *ProtoMarshaller) Marshall(v interface{}) ([]byte, error) {
	anyPB, err := anypb.New(v.(proto.Message))
	if err != nil {
		return nil, errors.WithPrevious(err, "failed to add message into anypb")
	}

	value, err := proto.Marshal(anyPB)
	if err != nil {
		return nil, errors.WithPrevious(err, "failed to marshal message into anypb")
	}

	return value, nil
}

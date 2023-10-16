/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package schemaregistry

// GenericEncoder holds the reference to Registry and Subject which can be used to encode and decode messages
type GenericEncoder struct {
	*Encoder
}

func (s *GenericEncoder) Encode(data interface{}) ([]byte, error) {
	panic(`generic encoder does not support encoding of messages`)
}

// Schema return the subject asociated with the Encoder
func (s *GenericEncoder) Schema() string {
	return `generic`
}

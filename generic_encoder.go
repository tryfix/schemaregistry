/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package schemaregistry

// GenericEncoder holds the reference to Registry and Subject which can be used to decode messages
//
// if err := registry.Register(`test-subject-avro`, schemaregistry.VersionAll,
//
//		func(unmarshaler schemaregistry.Unmarshaler) (v interface{}, err error) {
//	    re := SampleRecord{}
//	    if err := unmarshaler.Unmarshal(&re); err != nil {
//	        return nil, err
//	    }
//
//	    return re, nil
//	}); err != nil {
//
//	    log.Fatal(err)
//	}
//
//	// Encode logic...
//
//	ev, err := registry.GenericEncoder().Decode(payload) // Returns SampleRecord
//	if err != nil {
//	    panic(err)
//	}
type GenericEncoder struct {
	Encoder
}

func (s *GenericEncoder) Encode(_ interface{}) ([]byte, error) {
	panic(`generic encoder does not support encoding of messages`)
}

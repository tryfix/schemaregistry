package schema_registry

import (
	"fmt"
	"time"

	"github.com/riferrei/srclient"
	"github.com/tryfix/log"
	com_mycorp_mynamespace "github.com/tryfix/schemaregistry/v2/protobuf"
)

func Example_avro() {
	// Init a new schema registry instance and connect
	url := `http://localhost:8081/`
	registry, err := NewRegistry(
		url,
		WithLogger(log.NewLog().Log(log.WithLevel(log.TRACE))),
		WithBackgroundSync(5*time.Second),
		// MockClient for examples only
		WithMockClient(srclient.CreateMockSchemaRegistryClient(url)),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Start Background Sync to detect new Versions
	if err := registry.Sync(); err != nil {
		log.Fatal(err)
	}

	type SampleRecord struct {
		Field1 int     `avro:"field1"`
		Field2 float64 `avro:"field2"`
		Field3 string  `avro:"field3"`
	}

	subject := `test-subject-avro`
	if err := registry.Register(subject, 1, func(unmarshaler Unmarshaler) (v interface{}, err error) {
		record := SampleRecord{}
		if err := unmarshaler.Unmarshal(&record); err != nil {
			return nil, err
		}

		return record, nil
	}); err != nil {
		log.Fatal(err)
	}

	// Encode the message
	record := SampleRecord{
		Field1: 1,
		Field2: 2.0,
		Field3: "text",
	}

	bytePayload, err := registry.WithSchema(subject, 1).Encode(record)
	if err != nil {
		panic(err)
	}

	// Decode the message
	ev, err := registry.GenericEncoder().Decode(bytePayload) // Returns SampleRecord
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v", ev)
}

func Example_protobuf() {
	// Init a new schema registry instance and connect
	url := `http://localhost:8081/`
	registry, err := NewRegistry(
		url,
		WithBackgroundSync(5*time.Second),
		WithLogger(log.NewLog().Log(log.WithLevel(log.TRACE))),
		// MockClient for examples only
		WithMockClient(srclient.CreateMockSchemaRegistryClient(url)),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Start Background Sync to detect new Versions
	if err := registry.Sync(); err != nil {
		log.Fatal(err)
	}

	subject := `test-subject-protobuf`
	if err := registry.Register(subject, 1, func(unmarshaler Unmarshaler) (v interface{}, err error) {
		record := &com_mycorp_mynamespace.SampleRecord{}
		if err := unmarshaler.Unmarshal(&record); err != nil {
			return nil, err
		}

		return record, nil
	}); err != nil {
		log.Fatal(err)
	}

	// Encode the message
	record := &com_mycorp_mynamespace.SampleRecord{
		Field1: 1,
		Field2: 2.0,
		Field3: "text",
	}

	bytePayload, err := registry.WithSchema(subject, 1).Encode(record)
	if err != nil {
		panic(err)
	}

	// Decode the message
	ev, err := registry.GenericEncoder().Decode(bytePayload) // Returns *com_mycorp_mynamespace.SampleRecord
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v", ev)
}

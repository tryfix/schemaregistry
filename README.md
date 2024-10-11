# Schema Registry Client

[![GoDoc](https://godoc.org/github.com/tryfix/schemaregistry?status.svg)](https://godoc.org/github.com/tryfix/schemaregistry)

This repository contains wrapper function to communicate 
Confluent's Kafka schema registry via REST API and 
schema dynamic sync directly from kafka topic.

Client
------
Download library using `go get -u github.com/tryfix/schema-registry/v2`

Following code slice create a schema registry client 
```go
import schemaregistry "github.com/tryfix/schemaregistry"

registry, _ := NewRegistry(
		`http://localhost:8081/`,
		WithBackgroundSync(5*time.Second),
	)
```

Register an event `com.example.events.test` with version `1`
```go
import schemaregistry "github.com/tryfix/schemaregistry"

if err := registry.Register(`com.example.events.test`, 1, func(unmarshaler Unmarshaler) (v interface{}, err error) {
		record := SampleRecord{}
		if err := unmarshaler.Unmarshal(&record); err != nil {
			return nil, err
		}

		return record, nil
	}); err != nil {
		log.Fatal(err)
	}
```

Message encoding/decoding using above registered schema 
```go
// avro message structure
type SampleRecord struct {
Field1 int     `avro:"field1"`
Field2 float64 `avro:"field2"`
Field3 string  `avro:"field3"`
}

// Get encoder  
encoder := registry.WithSchema(`com.example.events.test`, 1)

// Sample message
record := SampleRecord{
Field1: 1,
Field2: 2.0,
Field3: "text",
}

// Message encode to byte array
bytePayload, err := encoder.Encode(record)
if err!=nil {
    panic(err)
}

// Decode message
ev, err := encoder.Decode(bytePayload)
if err != nil {
panic(err)
}
fmt.Printf("%+v", ev)
```

Message can be decoded through generic encoder as below

```go
// Decode message as generic encoder
ev, err := registry.GenericEncoder().Decode(bytePayload) // Returns SampleRecord
if err != nil {
panic(err)
}
```
Message Structure
-----------------
Encoded messages are published with magic byte and a schema ID attached to it.
Following structure shows the message format used in the library to encode the message.

    +====================+=== =================+======================+
    | Magic byte(1 byte) | Schema ID(4 bytes) | Payload              |
    +====================+====================+======================+

ToDo
----
 - write benchmarks

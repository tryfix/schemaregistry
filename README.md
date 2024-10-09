# Schema Registry Client

[![GoDoc](https://godoc.org/github.com/tryfix/schemaregistry?status.svg)](https://godoc.org/github.com/tryfix/schemaregistry)

This repository contains wrapper function to communicate 
Confluent's Kafka schema registry via REST API and 
schema dynamic sync directly from kafka topic.

Client
------
Download library using `go get -u github.com/tryfix/schema-registry`

Following code slice create a schema registry client 
```go
import schemaregistry "github.com/tryfix/schemaregistry"

registry, _ := schemaregistry.NewRegistry(`localhost:8089/`)
```

Following code line register an event `com.pickme.events.test` with version `1`
```go
import schemaregistry "github.com/tryfix/schemaregistry"

registry.Register(`com.organisation.events.test`, 1, func(data []byte) (v interface{}, err error)
```

Dynamically sync schema's from kafka schema data topic
```go
import schemaregistry "github.com/tryfix/schemaregistry"

registry, _ := schemaregistry.NewRegistry(`localhost:8089/`,schemaregistry.WithBackgroundSync([]string{`localhost:9092`}, `__schemas`))
registry.Sync()
```

Message Structure
-----------------
Encoded messages are published with magic byte and a schema ID attached to it.
Following structure shows the message format used in the library to encode the message. 

    +====================+====================+======================+
    | Magic byte(1 byte) | Schema ID(4 bytes) | Payload              |
    +====================+====================+======================+

ToDo
----
 - Write unit test
 - Write mock functions
 - write benchmarks
 - setup travis for automated testing

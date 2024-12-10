/*
Package schema_registry implements provides a generic Encoder/Decoder interface for Kafka Schema Registry

It hides the complexity of handling avro and protobuf packages by abstracting them with a generics Encoder interface.

# Features
  - Automatically detects and registeres new subject versions
  - Fetch and registeres schemas if not already registered

Schema registry API : https://docs.confluent.io/platform/current/schema-registry/develop/api.html

See the specific specifications for an understanding how encoding works.

Avro: http://avro.apache.org/docs/current/

Protobuf: https://protobuf.dev/programming-guides/encoding/
*/

package schemaregistry

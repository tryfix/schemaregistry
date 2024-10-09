package schema_registry

import (
	registry "github.com/riferrei/srclient"
	"github.com/tryfix/log"
	"os"
	"reflect"
	"testing"
	"time"
)

var testSchemas = map[string]string{}

func init() {
	bytAvro, err := os.ReadFile(`sample.avro`)
	if err != nil {
		panic(err)
	}

	bytAvroV2, err := os.ReadFile(`sample_v2.avro`)
	if err != nil {
		panic(err)
	}

	bytProto, err := os.ReadFile(`sample.proto`)
	if err != nil {
		panic(err)
	}

	testSchemas[`avro_v1`] = string(bytAvro)
	testSchemas[`avro_v2`] = string(bytAvroV2)
	testSchemas[`proto`] = string(bytProto)
}

type SampleV1 struct {
	Field1 int     `avro:"field1"`
	Field2 float64 `avro:"field2"`
	Field3 string  `avro:"field3"`
}

type SampleV2 struct {
	Field1 int     `avro:"field1"`
	Field2 float64 `avro:"field2"`
	Field3 string  `avro:"field3"`
	Field4 string  `avro:"field4"`
}

type mockRegistry struct {
	*Registry
	client *registry.MockSchemaRegistryClient
}

func setupMockRegistry(bgSyncInterval time.Duration) mockRegistry {
	mockClient := registry.CreateMockSchemaRegistryClient(`test`)
	reg, err := NewRegistry(`mock`, WithMockClient(mockClient),
		WithLogger(log.Constructor.Log(log.WithColors(false))),
		WithBackgroundSync(bgSyncInterval),
	)
	if err != nil {
		panic(err)
	}

	return mockRegistry{
		Registry: reg,
		client:   mockClient,
	}
}

func TestRegistry_GenericEncoder(t *testing.T) {
	reg := setupMockRegistry(1)
	_, err := reg.client.SetSchema(100, `test_subject`, testSchemas[`avro_v1`], registry.Avro, 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := reg.Register(`test_subject`, 1, func(unmarshaler Unmarshaler) (interface{}, error) {
		v := SampleV1{}
		if err := unmarshaler.Unmarshal(&v); err != nil {
			return nil, err
		}

		return v, nil
	}); err != nil {
		t.Fatal(err)
	}

	v := SampleV1{
		Field1: 100,
		Field2: 10.11,
		Field3: "text",
	}
	byt, err := reg.WithSchema(`test_subject`, 1).Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	vOut, err := reg.GenericEncoder().Decode(byt)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(v, vOut) {
		t.Errorf(`need %v, have %v`, v, vOut)
	}
}

func TestRegistry_WithLatestSchema(t *testing.T) {
	reg := setupMockRegistry(1)
	_, err := reg.client.SetSchema(100, `test_subject`, testSchemas[`avro_v1`], registry.Avro, 1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reg.client.SetSchema(100, `test_subject`, testSchemas[`avro_v2`], registry.Avro, 2)
	if err != nil {
		t.Fatal(err)
	}

	unmarsheller := func(unmarshaler Unmarshaler) (interface{}, error) {
		v := SampleV1{}
		if err := unmarshaler.Unmarshal(&v); err != nil {
			return nil, err
		}

		return v, nil
	}

	if err := reg.Register(`test_subject`, 1, unmarsheller); err != nil {
		t.Fatal(err)
	}

	if err := reg.Register(`test_subject`, 2, unmarsheller); err != nil {
		t.Fatal(err)
	}

	v := SampleV1{
		Field1: 100,
		Field2: 10.11,
		Field3: "text",
	}
	byt, err := reg.WithLatestSchema(`test_subject`).Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	vOut, err := reg.GenericEncoder().Decode(byt)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(v, vOut) {
		t.Errorf(`need %v, have %v`, v, vOut)
	}
}

func TestRegistry_WithSchema(t *testing.T) {
	reg := setupMockRegistry(1)
	_, err := reg.client.SetSchema(100, `test_subject`, testSchemas[`avro_v1`], registry.Avro, 1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reg.client.SetSchema(100, `test_subject`, testSchemas[`avro_v2`], registry.Avro, 2)
	if err != nil {
		t.Fatal(err)
	}

	if err := reg.Register(`test_subject`, 2, func(unmarshaler Unmarshaler) (interface{}, error) {
		v := SampleV1{}
		if err := unmarshaler.Unmarshal(&v); err != nil {
			return nil, err
		}

		return v, nil
	}); err != nil {
		t.Fatal(err)
	}

	v := SampleV1{
		Field1: 100,
		Field2: 10.11,
		Field3: "text",
	}
	byt, err := reg.WithSchema(`test_subject`, 2).Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	vOut, err := reg.WithSchema(`test_subject`, 2).Decode(byt)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(v, vOut) {
		t.Errorf(`need %v, have %v`, v, vOut)
	}
}

//func TestRegistry_WithSchemaProtobuf(t *testing.T) {
//	reg := setupMockRegistry(1)
//	_, err := reg.client.SetSchema(100, `test_subject`, testSchemas[`proto`], registry.Protobuf, 1)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	if err := reg.Register(`test_subject`, 2, func(unmarshaler Unmarshaler) (interface{}, error) {
//		v := &com_mycorp_mynamespace.SampleRecord{}
//		if err := unmarshaler.Unmarshal(v); err != nil {
//			return nil, err
//		}
//
//		return v, nil
//	}); err != nil {
//		t.Fatal(err)
//	}
//
//	v := &com_mycorp_mynamespace.SampleRecord{
//		Field1: 100,
//		Field2: 10.11,
//		Field3: "text",
//		Field4: "text 2",
//	}
//	byt, err := reg.WithSchema(`test_subject`, 2).Encode(v)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	vOut, err := reg.WithSchema(`test_subject`, 2).Decode(byt)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	if !reflect.DeepEqual(v, vOut) {
//		t.Errorf(`need %v, have %v`, v, vOut)
//	}
//}

func TestRegistry_NewVersionAdded(t *testing.T) {
	reg := setupMockRegistry(1)
	_, err := reg.client.SetSchema(100, `test_subject`, testSchemas[`avro_v1`], registry.Avro, 1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reg.client.SetSchema(101, `test_subject`, testSchemas[`avro_v2`], registry.Avro, 3)
	if err != nil {
		t.Fatal(err)
	}

	reg2 := setupMockRegistry(1)
	_, err = reg2.client.SetSchema(100, `test_subject`, testSchemas[`avro_v1`], registry.Avro, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reg2.client.SetSchema(101, `test_subject`, testSchemas[`avro_v2`], registry.Avro, 2)
	if err != nil {
		t.Fatal(err)
	}

	if err := reg.Register(`test_subject`, 1, func(unmarshaler Unmarshaler) (interface{}, error) {
		v := SampleV1{}
		if err := unmarshaler.Unmarshal(&v); err != nil {
			return nil, err
		}

		return v, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := reg2.Register(`test_subject`, 2, func(unmarshaler Unmarshaler) (interface{}, error) {
		v := SampleV1{}
		if err := unmarshaler.Unmarshal(&v); err != nil {
			return nil, err
		}

		return v, nil
	}); err != nil {
		t.Fatal(err)
	}

	v := SampleV1{
		Field1: 100,
		Field2: 10.11,
		Field3: "text",
	}
	byt, err := reg2.WithSchema(`test_subject`, 2).Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	vOut, err := reg.GenericEncoder().Decode(byt)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(v, vOut) {
		t.Errorf(`need %v, have %v`, v, vOut)
	}
}

func TestRegistry_NewVersionAddedShouldFailedForUnregisteredSubjects(t *testing.T) {
	reg := setupMockRegistry(1)
	_, err := reg.client.SetSchema(100, `test_subject`, testSchemas[`avro_v1`], registry.Avro, 1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reg.client.SetSchema(101, `test_subject`, testSchemas[`avro_v2`], registry.Avro, 3)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reg.client.SetSchema(500, `test_subject_other`, testSchemas[`avro_v2`], registry.Avro, 2)
	if err != nil {
		t.Fatal(err)
	}

	reg2 := setupMockRegistry(1)
	_, err = reg2.client.SetSchema(500, `test_subject_other`, testSchemas[`avro_v2`], registry.Avro, 2)
	if err != nil {
		t.Fatal(err)
	}

	if err := reg.Register(`test_subject`, 1, func(unmarshaler Unmarshaler) (interface{}, error) {
		v := SampleV1{}
		if err := unmarshaler.Unmarshal(&v); err != nil {
			return nil, err
		}

		return v, nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := reg2.Register(`test_subject_other`, 2, func(unmarshaler Unmarshaler) (interface{}, error) {
		v := SampleV1{}
		if err := unmarshaler.Unmarshal(&v); err != nil {
			return nil, err
		}

		return v, nil
	}); err != nil {
		t.Fatal(err)
	}

	v := SampleV1{
		Field1: 100,
		Field2: 10.11,
		Field3: "text",
	}
	byt, err := reg2.WithSchema(`test_subject_other`, 2).Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reg.GenericEncoder().Decode(byt)
	if err == nil {
		t.Fatal(err)
	}
}

func TestWithBackgroundSync(t *testing.T) {
	reg := setupMockRegistry(1 * time.Second)
	if err := reg.Sync(); err != nil {
		t.Fatal(err)
	}
	_, err := reg.client.SetSchema(100, `test_subject`, testSchemas[`avro_v1`], registry.Avro, 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := reg.Register(`test_subject`, 1, func(unmarshaler Unmarshaler) (interface{}, error) {
		v := SampleV1{}
		if err := unmarshaler.Unmarshal(&v); err != nil {
			return nil, err
		}

		return v, nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err = reg.client.SetSchema(101, `test_subject`, testSchemas[`avro_v2`], registry.Avro, 2)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	if reg.WithSchema(`test_subject`, 2) == nil {
		t.Fatal()
	}
}

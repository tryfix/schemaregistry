package schemaregistry

import (
	"bytes"
	"fmt"
	"github.com/tryfix/errors"
	"strings"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"

	registry "github.com/riferrei/srclient"
	"github.com/tryfix/log"
)

// Version is the type to hold default register vrsion options
type Version int

const (
	//VersionLatest constant hold the flag to register the latest version of the subject
	VersionLatest Version = -1
	//VersionAll constant hold the flag to register all the versions of the subject
	VersionAll Version = -2
)

// String returns the version type
func (v Version) String() string {
	if v == VersionLatest {
		return `Latest`
	}

	if v == VersionAll {
		return `All`
	}

	return fmt.Sprint(int(v))
}

type UnmarshalerFunc func(unmarshaler Unmarshaler) (interface{}, error)

// Subject holds the Schema information of the registered subject
type Subject struct {
	Schema          string  // The actual AVRO subject
	Subject         string  // Subject where the subject is registered for
	Version         Version // Version within this subject
	Id              int     // Registry's unique id
	UnmarshalerFunc UnmarshalerFunc
	marsheller      Marshaller
}

func (s Subject) String() string {
	return fmt.Sprintf(`%s#%d(Schema ID:%d)`, s.Subject, s.Version, s.Id)
}

type Options struct {
	backgroundSync struct {
		enabled      bool
		syncInterval time.Duration
	}
	logger     log.Logger
	mockClient *registry.MockSchemaRegistryClient
}

// Registry type holds schema registry details
type Registry struct {
	subjects     map[string]map[Version]*Subject
	unmarshalers map[string]UnmarshalerFunc
	idMap        map[int]*Subject
	client       registry.ISchemaRegistryClient
	mu           *sync.RWMutex
	options      *Options
	logger       log.Logger
}

// Option is a type to host NewRegistry configurations
type Option func(*Options)

// WithBackgroundSync Checks for new schemas by regularly querying schema registry
func WithBackgroundSync(syncInterval time.Duration) Option {
	return func(options *Options) {
		options.backgroundSync.syncInterval = syncInterval
		options.backgroundSync.enabled = true
	}
}

// WithLogger returns a Configurations to create a NewRegistry with given PrefixedLogger
func WithLogger(logger log.Logger) Option {
	return func(options *Options) {
		options.logger = logger
	}
}

// WithMockClient create a mock version of the registry(Testing purposes only)
func WithMockClient(client *registry.MockSchemaRegistryClient) Option {
	return func(options *Options) {
		options.mockClient = client
	}
}

// NewRegistry returns a Registry instance
func NewRegistry(url string, opts ...Option) (*Registry, error) {
	options := new(Options)
	options.logger = log.NewNoopLogger()
	options.backgroundSync.syncInterval = 10 * time.Second

	for _, opt := range opts {
		opt(options)
	}

	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}

	var client registry.ISchemaRegistryClient = registry.NewSchemaRegistryClient(url)

	if options.mockClient != nil {
		client = options.mockClient
	}

	r := &Registry{
		subjects:     make(map[string]map[Version]*Subject),
		unmarshalers: map[string]UnmarshalerFunc{},
		idMap:        make(map[int]*Subject),
		client:       client,
		mu:           new(sync.RWMutex),
		options:      options,
		logger:       options.logger.NewLog(log.Prefixed(`SchemaRegistryClient`)),
	}

	return r, nil
}

// Register registers the given subject, version and UnmarshalerFunc in the Registry
func (r *Registry) Register(subjectName string, version Version, unmarshalerFunc UnmarshalerFunc) error {
	if _, ok := r.subjects[subjectName]; ok {
		if _, ok := r.subjects[subjectName][version]; ok {
			r.logger.Warn(fmt.Sprintf(`Subject [%s][%s] already registred`, subjectName, version))
		}
	}

	if version == VersionAll {
		versions, err := r.client.GetSchemaVersions(subjectName)
		if err != nil {
			return errors.WithPrevious(err, fmt.Sprintf(`Fetching schema versions for %s:%s failed.`, subjectName, version))
		}
		for _, v := range versions {
			if err := r.Register(subjectName, Version(v), unmarshalerFunc); err != nil {
				return err
			}
		}
		return nil
	}

	var clientSub *registry.Schema
	if version == VersionLatest {
		sub, err := r.client.GetLatestSchema(subjectName)
		if err != nil {
			return errors.WithPrevious(err, fmt.Sprintf(`Fetching latest schema for %s failed.`, subjectName))
		}

		clientSub = sub
	} else {
		sub, err := r.client.GetSchemaByVersion(subjectName, int(version))
		if err != nil {
			return errors.WithPrevious(err, fmt.Sprintf(`Fetching schema for %s:%s failed.`, subjectName, version))
		}

		clientSub = sub
	}

	subject := &Subject{
		Schema:          clientSub.Schema(),
		Id:              clientSub.ID(),
		Version:         Version(clientSub.Version()),
		Subject:         subjectName,
		UnmarshalerFunc: unmarshalerFunc,
	}

	subject.marsheller = r.getMarshaller(clientSub.SchemaType(), clientSub.Schema())

	if err := subject.marsheller.Init(); err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`Initiating Marshaller for schema %s:%s failed.`, subject, version))
	}

	if _, ok := r.subjects[subjectName]; !ok {
		r.subjects[subjectName] = map[Version]*Subject{}
	}

	r.unmarshalers[subjectName] = unmarshalerFunc
	r.subjects[subjectName][version] = subject
	r.idMap[clientSub.ID()] = subject

	r.logger.Info(fmt.Sprintf(`Subject %s registred`, subject))

	return nil
}

// Sync function starts the background process looking for news schema versions for already registered subjects
//
// Newly Created Schemas will register in background and the client does not need any restarts
func (r *Registry) Sync() error {
	if r.options.backgroundSync.enabled {
		err := Sync(r.options.backgroundSync.syncInterval, r.logger, r)
		if err != nil {
			return err
		}
	}

	return nil
}

// WithSchema return the specific encoder which registered at the initialization under the subject and version
func (r *Registry) WithSchema(subject string, version Version) Encoder {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.subjects[subject][version]
	if !ok {
		panic(fmt.Sprintf(`schemaregistry.registry: unregistred subject %s:%d`, subject, version))
	}

	return NewRegistryEncoder(r, e)
}

// WithLatestSchema returns the latest event version encoder registered under given subject
func (r *Registry) WithLatestSchema(subject string) Encoder {
	r.mu.Lock()
	defer r.mu.Unlock()

	versions, ok := r.subjects[subject]
	if !ok {
		panic(fmt.Sprintf(`schemaregistry.registry: unregistred subject [%s]`, subject))
	}
	var v Version
	for _, version := range versions {
		if version.Version > v {
			v = version.Version
		}
	}

	return NewRegistryEncoder(r, versions[v])
}

// GenericEncoder returns a placeholder encoder for decoders.
// It can be used to decode any schema version for registered subjects without explicitly mentioning
// the Subject:Version combination
func (r *Registry) GenericEncoder() Encoder {
	return &GenericEncoder{NewRegistryEncoder(r, nil)}
}

func (r *Registry) getSubjectBySchemaID(schemaID int) (subject *Subject, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subject, ok = r.idMap[schemaID]

	return
}

func (r *Registry) getUnMarshallerFunc(subjectName string) UnmarshalerFunc {
	r.mu.RLock()
	defer r.mu.RUnlock()

	unmarshallar, ok := r.unmarshalers[subjectName]
	if !ok {
		panic(fmt.Sprintf(`un marshaller doesn't exists for subject %s`, subjectName))
	}

	return unmarshallar
}

func (r *Registry) getMarshaller(schemaType *registry.SchemaType, schema string) Marshaller {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var marshaller Marshaller = NewAvroMarshaller(schema)
	if schemaType != nil && *schemaType == registry.Protobuf {
		marshaller = NewProtoMarshaller()
	}

	return marshaller
}

func (r *Registry) addSubjectBySchema(schema *registry.Schema, subjectName string) error {
	subject := &Subject{
		Subject:         subjectName,
		Version:         Version(schema.Version()),
		Schema:          schema.Schema(),
		Id:              schema.ID(),
		UnmarshalerFunc: r.getUnMarshallerFunc(subjectName),
	}

	subject.marsheller = r.getMarshaller(schema.SchemaType(), subject.Schema)
	if err := subject.marsheller.Init(); err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`Initiating Marshaller for schema %s:%d failed.`, subject, schema.Version()))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.subjects[subject.Subject][subject.Version] = subject
	r.idMap[subject.Id] = subject

	return nil
}

func (r *Registry) subjectRegistered(subject string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.subjects[subject]

	return ok
}

func (r *Registry) hasVersion(subject string, version Version) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.subjects[subject]
	if !ok {
		panic(fmt.Sprintf(`subject %s not registered`, subject))
	}

	_, versionExists := r.subjects[subject][version]

	return versionExists
}

func (r *Registry) updateRegistryCache(schemaID int) error {
	schema, err := r.client.GetSchema(schemaID)
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`fetch schema failed for Schama ID: %d`, schemaID))
	}

	resp, err := r.client.GetSubjectVersionsById(schemaID)
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`fetch schema failed for Schama ID: %d`, schemaID))
	}

	subjectname := resp[0].Subject

	// Check if subject is registered
	if !r.subjectRegistered(subjectname) {
		return errors.New(fmt.Sprintf(
			`Schema ID - %d cannot be added to the Registry. Subject %s not registered`, schemaID, subjectname))
	}

	return r.addSubjectBySchema(schema, subjectname)
}

func (r *Registry) Print(subject *Subject) {
	b := new(bytes.Buffer)
	table := tablewriter.NewWriter(b)
	table.SetHeader([]string{`Schema Id`, `subject`, `version`, `decoderFunc`})
	table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
	table.SetAutoFormatHeaders(true)

	appendRow := func(subject *Subject) {
		table.Append([]string{
			fmt.Sprint(subject.Id),
			fmt.Sprint(subject.Subject),
			fmt.Sprint(subject.Version),
			fmt.Sprint(subject.UnmarshalerFunc != nil),
		})
	}

	if subject != nil {
		appendRow(subject)
	} else {
		for _, versions := range r.subjects {
			for _, version := range versions {
				appendRow(version)
			}
		}
	}

	table.Render()
	r.logger.Info(fmt.Sprintf("Schemas\n%s", b.String()))
}

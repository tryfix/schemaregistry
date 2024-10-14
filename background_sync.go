package schema_registry

import (
	"fmt"
	"github.com/tryfix/log"
	"time"
)

type backgroundSync struct {
	syncInterval time.Duration
	registry     *Registry
	synced       bool
	logger       log.Logger
}

func Sync(syncInterval time.Duration, logger log.Logger, registry *Registry) error {
	sync := &backgroundSync{
		registry:     registry,
		syncInterval: syncInterval,
		logger:       logger.NewLog(log.Prefixed(`BGSync`)),
	}

	ticker := time.NewTicker(sync.syncInterval)

	registry.Print(nil)

	go func() {
		for range ticker.C {
			sync.checkRegistryAndAdd()
		}
	}()

	sync.logger.Debug(`New Schema check background routine started`)

	return nil

}

func (s *backgroundSync) checkRegistryAndAdd() {
	s.logger.Debug(`Looking for new Schemas...`)
	added := 0
	defer func() {
		s.logger.Debug(fmt.Sprintf(`Looking for new Schemas completed, %d schema/s added`, added))
	}()

	// Fetch schemas
	subjects, err := s.registry.client.GetSubjects()
	if err != nil {
		s.logger.Error(fmt.Sprintf(`Error getting subjects due to %s`, err.Error()))
		return
	}

	// If the subject is registered, check for new versions
	for _, subjectName := range subjects {
		if s.registry.subjectRegistered(subjectName) {
			// Fetch versions
			versions, err := s.registry.client.GetSchemaVersions(subjectName)
			if err != nil {
				s.logger.Error(fmt.Sprintf(`Error getting schema versions due to %s`, err.Error()))
				continue
			}

			for _, version := range versions {
				if !s.registry.hasVersion(subjectName, Version(version)) {
					// Fetch versions
					schema, err := s.registry.client.GetSchemaByVersion(subjectName, version)
					if err != nil {
						s.logger.Error(fmt.Sprintf(`Error getting schema by version due to %s`, err.Error()))
						continue
					}

					subject := &Subject{
						Subject:         subjectName,
						Version:         Version(schema.Version()),
						Schema:          schema.Schema(),
						Id:              schema.ID(),
						UnmarshalerFunc: s.registry.getUnMarshallerFunc(subjectName),
					}

					if err := s.registry.addSubjectBySchema(schema, subjectName); err != nil {
						s.logger.Error(fmt.Sprintf("New Schema add failed. [%s:%d] due to %s",
							subjectName, schema.Version(), err.Error()))
						continue
					}

					s.logger.Info(fmt.Sprintf("New Schema registered. %s:%d", subjectName, schema.Version()))

					s.registry.Print(subject)
					added++
				}
			}

		}
	}
}

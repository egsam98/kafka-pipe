package pg

import (
	"github.com/dgraph-io/badger/v4"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type SourceConfig struct {
	Name string `yaml:"name" validate:"required"`
	Pg   struct {
		SkipDelete  bool     `yaml:"skip.delete"`
		Url         string   `yaml:"url" validate:"url"`
		Publication string   `yaml:"publication" validate:"required"`
		Slot        string   `yaml:"slot" validate:"required"`
		Tables      []string `yaml:"tables" validate:"min=1"`
		HealthTable string   `yaml:"health.table" validate:"default=public.pipe_health"`
	} `yaml:"pg"`
	Kafka   kafkapipe.ProducerConfig `yaml:"kafka"`
	Storage *badger.DB               `yaml:"-"`
}

type SnapshotConfig struct {
	Pg struct {
		Url       string   `yaml:"url" validate:"url"`
		Tables    []string `yaml:"tables" validate:"min=1"`
		Condition string   `yaml:"condition"`
	} `yaml:"pg"`
	Kafka kafkapipe.ProducerConfig `yaml:"kafka"`
}

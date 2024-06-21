package pg

import (
	"time"

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
	Kafka struct {
		Brokers []string `yaml:"brokers" validate:"min=1,dive,url"`
		Topic   struct {
			Prefix            string `yaml:"prefix"`
			ReplicationFactor uint16 `yaml:"replication.factor" validate:"default=1"`
			Partitions        uint32 `yaml:"partitions" validate:"default=1"`
			CleanupPolicy     string `yaml:"cleanup.policy" validate:"default=delete"`
			CompressionType   string `yaml:"compression.type" validate:"default=producer"`
		} `yaml:"topic"`
		Batch struct {
			Size    uint          `yaml:"size" validate:"default=10000"`
			Timeout time.Duration `yaml:"timeout" validate:"default=5s"`
		} `yaml:"batch"`
	} `yaml:"kafka"`
}

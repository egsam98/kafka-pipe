package source

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Config struct {
	Name string `yaml:"name"`
	Pg   struct {
		SkipDelete  bool     `yaml:"skip.delete"`
		Url         string   `yaml:"url" validate:"url"`
		Publication string   `yaml:"publication" validate:"required"`
		Slot        string   `yaml:"slot" validate:"required"`
		Tables      []string `yaml:"tables" validate:"min=1"`
		HealthTable string   `yaml:"health.table" validate:"default=public.pipe_health"`
	} `yaml:"pg"`
	Kafka struct {
		Brokers []string `yaml:"brokers" validate:"min=1,dive,url"`
		Topic   struct {
			Prefix            string            `yaml:"prefix"`
			ReplicationFactor int16             `yaml:"replication.factor" validate:"default=1"`
			Partitions        int32             `yaml:"partitions" validate:"default=1"`
			CleanupPolicy     string            `yaml:"cleanup.policy" validate:"default=delete"`
			CompressionType   string            `yaml:"compression.type" validate:"default=producer"`
			Routes            map[string]string `yaml:"routes"`
		} `yaml:"topic"`
		Batch struct {
			Size    int           `yaml:"size" validate:"default=10000"`
			Timeout time.Duration `yaml:"timeout" validate:"default=5s"`
		} `yaml:"batch"`
	} `yaml:"kafka"`
	Storage *badger.DB `yaml:"-"`
}

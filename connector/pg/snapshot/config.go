package snapshot

import (
	"time"
)

type Config struct {
	Pg struct {
		Url       string   `yaml:"url" validate:"url"`
		Tables    []string `yaml:"tables" validate:"min=1"`
		Condition string   `yaml:"condition"`
	} `yaml:"pg"`
	Kafka struct {
		Brokers []string `yaml:"brokers" validate:"min=1,dive,url"`
		Topic   struct {
			Prefix            string `yaml:"prefix"`
			ReplicationFactor int16  `yaml:"replication.factor" validate:"default=1"`
			Partitions        int32  `yaml:"partitions" validate:"default=1"`
			CleanupPolicy     string `yaml:"cleanup.policy" validate:"default=delete"`
			CompressionType   string `yaml:"compression.type" validate:"default=producer"`
		} `yaml:"topic"`
		Batch struct {
			Size    int           `yaml:"size" validate:"default=10000"`
			Timeout time.Duration `yaml:"timeout" validate:"default=5s"`
		} `yaml:"batch"`
	} `yaml:"kafka"`
}

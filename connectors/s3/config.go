package s3

import (
	"time"

	"github.com/dgraph-io/badger/v4"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type SinkConfig struct {
	Name              string                       `yaml:"name" validate:"required"`
	Kafka             kafkapipe.ConsumerPoolConfig `yaml:"kafka"`
	S3                ConnConfig                   `yaml:"s3"`
	GroupTimeInterval time.Duration                `yaml:"group_time_interval" validate:"default=1h"`
	DB                *badger.DB                   `yaml:"-" validate:"required"`
}

type ConnConfig struct {
	SSL    bool   `yaml:"ssl"`
	URL    string `yaml:"url" validate:"url"`
	Bucket string `yaml:"bucket" validate:"required"`
	ID     string `yaml:"id" validate:"required"`
	Secret string `yaml:"secret" validate:"required"`
}

type BackupConfig struct {
	Kafka     kafkapipe.ProducerConfig `yaml:"kafka"`
	S3        ConnConfig               `yaml:"s3"`
	Topics    []string                 `yaml:"topics" validate:"min=1"`
	DateSince time.Time                `yaml:"-" validate:"required"`
	DateTo    time.Time                `yaml:"-" validate:"required"`
	DB        *badger.DB               `yaml:"-" validate:"required"`
}

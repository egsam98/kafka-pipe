package s3

import (
	"time"

	"github.com/dgraph-io/badger/v4"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type SinkConfig struct {
	// [warden]
	// required = true
	Name string `yaml:"name"`
	// [warden]
	// dive = true
	Kafka kafkapipe.ConsumerPoolConfig `yaml:"kafka"`
	// [warden]
	// dive = true
	S3 ConnConfig `yaml:"s3"`
	// [warden]
	// default = "1h"
	GroupTimeInterval time.Duration `yaml:"group_time_interval"`
	// [warden]
	// required = true
	DB *badger.DB `yaml:"-"`
}

type ConnConfig struct {
	SSL bool `yaml:"ssl"`
	// [warden]
	// required = true
	Endpoint string `yaml:"endpoint"`
	// [warden]
	// required = true
	Bucket string `yaml:"bucket"`
	// [warden]
	// required = true
	ID string `yaml:"id"`
	// [warden]
	// required = true
	Secret string `yaml:"secret"`
}

type BackupConfig struct {
	// [warden]
	// required = true
	Name string `yaml:"name"`
	// [warden]
	// dive = true
	Kafka KafkaConfig `yaml:"kafka"`
	// dive = true
	S3 ConnConfig `yaml:"s3"`
	// [warden]
	// non-empty = true
	Topics []string `yaml:"topics"`
	// [warden]
	// required = true
	DateSince time.Time `yaml:"-"`
	// [warden]
	// required = true
	DateTo time.Time `yaml:"-"`
	// [warden]
	// required = true
	DB *badger.DB `yaml:"-"`
}

type KafkaConfig struct {
	// [warden]
	// non-empty = true
	// [warden.dive]
	// url = true
	Brokers []string `yaml:"brokers"`
	// [warden]
	// dive = true
	Batch kafkapipe.BatchConfig `yaml:"batch"`
}

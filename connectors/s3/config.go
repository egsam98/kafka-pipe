package s3

import (
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/egsam98/ecto"
	ectosl "github.com/egsam98/ecto/slices"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type SinkConfig struct {
	Name              string                       `yaml:"name"`
	Kafka             kafkapipe.ConsumerPoolConfig `yaml:"kafka"`
	S3                ConnConfig                   `yaml:"s3"`
	GroupTimeInterval time.Duration                `yaml:"group_time_interval"`
	DB                *badger.DB                   `yaml:"-"`
}

var sinkCfgSchema = ecto.Struct[SinkConfig](ecto.M{
	"Name":              ecto.String().Required(),
	"Kafka":             kafkapipe.ConsumerPoolCfgSchema,
	"S3":                connCfgSchema,
	"GroupTimeInterval": ecto.Atomic[time.Duration]().Default(time.Hour),
	"DB":                ecto.Atomic[*badger.DB]().Required(),
})

type ConnConfig struct {
	SSL      bool   `yaml:"ssl"`
	Endpoint string `yaml:"endpoint"`
	Bucket   string `yaml:"bucket"`
	ID       string `yaml:"id"`
	Secret   string `yaml:"secret"`
}

var connCfgSchema = ecto.Struct[ConnConfig](ecto.M{
	"Endpoint": ecto.String().Required(),
	"Bucket":   ecto.String().Required(),
	"ID":       ecto.String().Required(),
	"Secret":   ecto.String().Required(),
})

type BackupConfig struct {
	Name      string      `yaml:"name"`
	Kafka     KafkaConfig `yaml:"kafka"`
	S3        ConnConfig  `yaml:"s3"`
	Topics    []string    `yaml:"topics"`
	DateSince time.Time   `yaml:"-"`
	DateTo    time.Time   `yaml:"-"`
	DB        *badger.DB  `yaml:"-"`
}

var backupCfgSchema = ecto.Struct[BackupConfig](ecto.M{
	"Name": ecto.String().Required(),
	"Kafka": ecto.Struct[KafkaConfig](ecto.M{
		"Brokers": ecto.Slice[[]string](ecto.String().Required()).
			Test(ectosl.Min[[]string](1)),
		"Batch": kafkapipe.BatchCfgSchema,
	}),
	"S3":        connCfgSchema,
	"Topics":    ecto.Slice[[]string](ecto.String()).Test(ectosl.Min[[]string](1)),
	"DateSince": ecto.Atomic[time.Time]().Required(),
	"DateTo":    ecto.Atomic[time.Time]().Required(),
	"DB":        ecto.Atomic[*badger.DB]().Required(),
})

type KafkaConfig struct {
	Brokers []string              `yaml:"brokers"`
	Batch   kafkapipe.BatchConfig `yaml:"batch"`
}

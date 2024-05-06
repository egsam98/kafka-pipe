package ch

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkapipe "github.com/egsam98/kafka-pipe"
	"github.com/egsam98/kafka-pipe/serde"
)

type SinkConfig struct {
	Name         string                                               `yaml:"name" validate:"required"`
	Kafka        kafkapipe.ConsumerPoolConfig                         `yaml:"kafka"`
	ClickHouse   ClickHouseConfig                                     `yaml:"click_house"`
	Serde        serde.Serde                                          `yaml:"-" validate:"required"`
	DB           *badger.DB                                           `yaml:"-" validate:"required"`
	BeforeInsert func(ctx context.Context, batch []*kgo.Record) error `yaml:"-"`
}

type ClickHouseConfig struct {
	Database string   `yaml:"database" validate:"required"`
	User     string   `yaml:"user" validate:"required"`
	Password string   `yaml:"password"`
	Addrs    []string `yaml:"addrs" validate:"min=1,dive,url"`
}

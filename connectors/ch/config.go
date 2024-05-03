package ch

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/egsam98/kafka-pipe/serde"
)

type SinkConfig struct {
	Name  string `yaml:"name" validate:"required"`
	Kafka struct {
		Brokers                []string      `yaml:"brokers" validate:"min=1,dive,url"`
		Topics                 []string      `yaml:"topics" validate:"min=1"`
		RebalanceTimeout       time.Duration `yaml:"rebalance_timeout" validate:"default=1m"`
		WorkersPerTopic        uint          `yaml:"workers_per_topic" validate:"default=1"`
		FetchMaxBytes          uint          `yaml:"fetch_max_bytes"`
		FetchMaxPartitionBytes uint          `yaml:"fetch_max_partition_bytes"`
		Batch                  struct {
			Size    uint          `yaml:"size" validate:"default=10000"`
			Timeout time.Duration `yaml:"timeout" validate:"default=5s"`
		} `yaml:"batch"`
	} `yaml:"kafka"`
	ClickHouse struct {
		Database string   `yaml:"database" validate:"required"`
		User     string   `yaml:"user" validate:"required"`
		Password string   `yaml:"password"`
		Addrs    []string `yaml:"addrs" validate:"min=1,dive,url"`
	} `yaml:"click_house"`
	Serde        serde.Serde                                          `yaml:"-" validate:"required"`
	DB           *badger.DB                                           `yaml:"-" validate:"required"`
	BeforeInsert func(ctx context.Context, batch []*kgo.Record) error `yaml:"-"`
}

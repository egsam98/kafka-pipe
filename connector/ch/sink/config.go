package sink

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Kafka struct {
		GroupID          string        `yaml:"group_id" validate:"required"`
		Brokers          []string      `yaml:"brokers" validate:"min=1,dive,url"`
		Topics           []string      `yaml:"topics" validate:"min=1"`
		RebalanceTimeout time.Duration `yaml:"rebalance_timeout" validate:"default=1m"`
		WorkersPerTopic  uint          `yaml:"workers_per_topic" validate:"default=1"`
		Batch            struct {
			Size    int           `yaml:"size" validate:"default=10000"`
			Timeout time.Duration `yaml:"timeout" validate:"default=5s"`
		} `yaml:"batch"`
	} `yaml:"kafka"`
	ClickHouse struct {
		Database string   `yaml:"database" validate:"required"`
		User     string   `yaml:"user" validate:"required"`
		Password string   `yaml:"password"`
		Addrs    []string `yaml:"addrs" validate:"min=1,dive,url"`
	} `yaml:"click_house"`
	OnProcess func(ctx context.Context, batch []*kgo.Record) error
}

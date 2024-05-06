package kafkapipe

import (
	"context"
	"time"
)

var Version = "dev"

type Connector interface {
	Run(ctx context.Context) error
}

type ConsumerPoolConfig struct {
	GroupPrefix            string        `yaml:"group_prefix" validate:"required"`
	Brokers                []string      `yaml:"brokers" validate:"min=1,dive,url"`
	Topics                 []string      `yaml:"topics" validate:"min=1"`
	RebalanceTimeout       time.Duration `yaml:"rebalance_timeout" validate:"default=1m"`
	WorkersPerTopic        uint          `yaml:"workers_per_topic" validate:"default=1"`
	FetchMaxBytes          uint          `yaml:"fetch_max_bytes"`
	FetchMaxPartitionBytes uint          `yaml:"fetch_max_partition_bytes"`
	Batch                  BatchConfig   `yaml:"batch"`
}

type BatchConfig struct {
	Size    uint          `yaml:"size" validate:"default=10000"`
	Timeout time.Duration `yaml:"timeout" validate:"default=5s"`
}

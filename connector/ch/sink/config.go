package sink

import (
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"kafka-pipe/internal/validate"
)

type Config struct {
	Kafka struct {
		GroupID          string        `yaml:"group_id" validate:"required"`
		Brokers          []string      `yaml:"brokers" validate:"required"`
		Topics           []string      `yaml:"topics" validate:"required"`
		RebalanceTimeout time.Duration `yaml:"rebalance_timeout" default:"1m"`
		Batch            struct {
			Size    int           `yaml:"size" validate:"default=10000"`
			Timeout time.Duration `yaml:"timeout" validate:"default=5s"`
		} `yaml:"batch"`
	} `yaml:"kafka"`
	ClickHouse struct {
		Database string   `yaml:"database" validate:"required"`
		User     string   `yaml:"user" validate:"required"`
		Password string   `yaml:"password"`
		Addrs    []string `yaml:"addrs" validate:"required"`
	} `yaml:"click_house"`
}

func (c *Config) Parse(src []byte) error {
	if err := yaml.Unmarshal(src, c); err != nil {
		return errors.Wrap(err, "parse sink config")
	}
	return validate.Struct(c)
}

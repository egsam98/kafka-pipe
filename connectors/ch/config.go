package ch

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

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
	ClickHouse ClickHouseConfig `yaml:"click_house"`
	// [warden]
	// required = true
	Serde kafkapipe.Serde `yaml:"-"`
	// [warden]
	// required = true
	DB           *badger.DB        `yaml:"-"`
	Routes       map[string]string `yaml:"routes"`
	BeforeInsert BeforeInsert      `yaml:"-"`
}

func (c *SinkConfig) UnmarshalYAML(node *yaml.Node) error {
	type inline SinkConfig // Avoid stack overflow
	var cfg struct {
		inline `yaml:",inline"`
		Serde  yaml.Node `yaml:"serde"`
	}
	if err := node.Decode(&cfg); err != nil {
		return err
	}

	*c = SinkConfig(cfg.inline)
	var err error
	c.Serde, err = kafkapipe.NewSerdeFromYAML(cfg.Serde)
	return err
}

type BeforeInsert func(ctx context.Context, serde kafkapipe.Serde, topic string, batch []*kgo.Record) ([]any, error)

type ClickHouseConfig struct {
	// [warden]
	// required = true
	Database string `yaml:"database"`
	// [warden]
	// required = true
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	// [warden]
	// non-empty = true
	// [warden.dive]
	// url = true
	Addrs []string `yaml:"addrs"`
}

package ch

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/egsam98/ecto"
	ectosl "github.com/egsam98/ecto/slices"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type SinkConfig struct {
	Name         string                       `yaml:"name"`
	Kafka        kafkapipe.ConsumerPoolConfig `yaml:"kafka"`
	ClickHouse   ClickHouseConfig             `yaml:"click_house"`
	Serde        kafkapipe.Serde              `yaml:"-"`
	DB           *badger.DB                   `yaml:"-"`
	Routes       map[string]string            `yaml:"routes"`
	BeforeInsert BeforeInsert                 `yaml:"-"`
}

var sinkCfgSchema = ecto.Struct[SinkConfig](ecto.M{
	"Name":       ecto.String().Required(),
	"Kafka":      kafkapipe.ConsumerPoolCfgSchema,
	"ClickHouse": clickHouseCfgSchema,
	"Serde":      ecto.Atomic[kafkapipe.Serde]().Required(),
	"DB":         ecto.Atomic[*badger.DB]().Required(),
})

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
	Database string   `yaml:"database"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Addrs    []string `yaml:"addrs"`
}

var clickHouseCfgSchema = ecto.Struct[ClickHouseConfig](ecto.M{
	"Database": ecto.String().Required(),
	"User":     ecto.String().Required(),
	"Addrs": ecto.Slice[[]string](
		ecto.String().Required(),
	).Test(ectosl.Min[[]string](1)),
})

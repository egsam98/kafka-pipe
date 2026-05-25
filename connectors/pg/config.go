package pg

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/egsam98/ecto"
	ectosl "github.com/egsam98/ecto/slices"
	ectos "github.com/egsam98/ecto/strings"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type SourceConfig struct {
	Name  string                   `yaml:"name"`
	Pg    ConnReplConfig           `yaml:"pg"`
	Kafka kafkapipe.ProducerConfig `yaml:"kafka"`
	DB    *badger.DB               `yaml:"-"`
}

var sourceCfgSchema = ecto.Struct[SourceConfig](ecto.M{
	"Name": ecto.String().Required(),
	"Pg": ecto.Struct[ConnReplConfig](ecto.M{
		"Url":         ecto.String().Test(ectos.URL()),
		"Publication": ecto.String().Required(),
		"Slot":        ecto.String().Required(),
		"Tables":      ecto.Slice[[]string](ecto.String()).Test(ectosl.Min[[]string](1)),
		"HealthTable": ecto.String().Default("public.pipe_health"),
	}),
	"Kafka": kafkapipe.ProducerCfgSchema,
	"DB":    ecto.Atomic[*badger.DB]().Required(),
})

type ConnReplConfig struct {
	SkipDelete  bool     `yaml:"skip.delete"`
	Url         string   `yaml:"url"`
	Publication string   `yaml:"publication"`
	Slot        string   `yaml:"slot"`
	Tables      []string `yaml:"tables"`
	HealthTable string   `yaml:"health.table"`
}

type SnapshotConfig struct {
	Pg    ConnConfig               `yaml:"pg"`
	Kafka kafkapipe.ProducerConfig `yaml:"kafka"`
}

var snapshotCfgSchema = ecto.Struct[SnapshotConfig](ecto.M{
	"Pg": ecto.Struct[ConnConfig](ecto.M{
		"Url":    ecto.String().Test(ectos.URL()),
		"Tables": ecto.Slice[[]string](ecto.String()).Test(ectosl.Min[[]string](1)),
	}),
	"Kafka": kafkapipe.ProducerCfgSchema,
})

type ConnConfig struct {
	Url       string   `yaml:"url"`
	Tables    []string `yaml:"tables"`
	Condition string   `yaml:"condition"`
}

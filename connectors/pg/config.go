package pg

import (
	"github.com/dgraph-io/badger/v4"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type SourceConfig struct {
	// [warden]
	// required = true
	Name string `yaml:"name"`
	// [warden]
	// dive = true
	Pg struct {
		SkipDelete bool `yaml:"skip.delete"`
		// [warden]
		// url = true
		Url string `yaml:"url"`
		// [warden]
		// required = true
		Publication string `yaml:"publication"`
		// [warden]
		// required = true
		Slot string `yaml:"slot"`
		// [warden]
		// non-empty = true
		Tables []string `yaml:"tables"`
		// [warden]
		// default = "public.pipe_health"
		HealthTable string `yaml:"health.table"`
	} `yaml:"pg"`
	// [warden]
	// dive = true
	Kafka kafkapipe.ProducerConfig `yaml:"kafka"`
	// [warden]
	// required = true
	DB *badger.DB `yaml:"-"`
}

type SnapshotConfig struct {
	// [warden]
	// dive = true
	Pg struct {
		// [warden]
		// url = true
		Url string `yaml:"url"`
		// [warden]
		// non-empty = true
		Tables    []string `yaml:"tables"`
		Condition string   `yaml:"condition"`
	} `yaml:"pg"`
	// [warden]
	// dive = true
	Kafka kafkapipe.ProducerConfig `yaml:"kafka"`
}

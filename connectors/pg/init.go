package pg

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	kafkapipe "github.com/egsam98/kafka-pipe"
	"github.com/egsam98/kafka-pipe/internal/registry"
)

func init() {
	registry.Register("pg.Source", func(config registry.Config) (kafkapipe.Connector, error) {
		var cfg SourceConfig
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse source config")
		}
		cfg.Storage = config.Storage
		return NewSource(cfg), nil
	})
	registry.Register("pg.Snapshot", func(config registry.Config) (kafkapipe.Connector, error) {
		var cfg SnapshotConfig
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse snapshot config")
		}
		return NewSnapshot(cfg), nil
	})
}

package source

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/egsam98/kafka-pipe/connector"
)

func init() {
	connector.Register("pg.Source", func(config connector.Config) (connector.Connector, error) {
		var cfg Config
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse source config")
		}
		cfg.Storage = config.Storage
		return NewSource(cfg), nil
	})
}

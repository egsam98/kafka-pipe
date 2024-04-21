package snapshot

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/egsam98/kafka-pipe/connector"
)

func init() {
	connector.Register("pg.Snapshot", func(config connector.Config) (connector.Connector, error) {
		var cfg Config
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse snapshot config")
		}
		return NewSnapshot(cfg), nil
	})
}

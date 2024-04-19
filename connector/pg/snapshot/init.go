package snapshot

import (
	"github.com/egsam98/kafka-pipe/connector"
)

func init() {
	connector.Register("pg.Snapshot", func(config connector.Config) (connector.Connector, error) {
		var cfg Config
		if err := cfg.Parse(config.Raw); err != nil {
			return nil, err
		}
		return NewSnapshot(cfg), nil
	})
}

package source

import (
	"kafka-pipe/connector"
)

func init() {
	connector.Register("pg.Source", func(config connector.Config) (connector.Connector, error) {
		var cfg Config
		if err := cfg.Parse(config.Raw); err != nil {
			return nil, err
		}
		cfg.Storage = config.Storage
		return NewSource(cfg)
	})
}

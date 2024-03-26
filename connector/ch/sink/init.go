package sink

import (
	"kafka-pipe/connector"
)

func init() {
	connector.Register("ch.Sink", func(config connector.Config) (connector.Connector, error) {
		var cfg Config
		if err := cfg.Parse(config.Raw); err != nil {
			return nil, err
		}
		return NewSink(cfg), nil
	})
}

package sink

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"kafka-pipe/connector"
)

func init() {
	connector.Register("ch.Sink", func(config connector.Config) (connector.Connector, error) {
		var cfg Config
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse sink config")
		}
		return NewSink(cfg), nil
	})
}

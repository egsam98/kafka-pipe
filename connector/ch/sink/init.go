package sink

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/egsam98/kafka-pipe/connector"
	"github.com/egsam98/kafka-pipe/serde"
)

func init() {
	connector.Register("ch.Sink", func(config connector.Config) (connector.Connector, error) {
		var cfg struct {
			Config `yaml:",inline"`
			Serde  yaml.Node `yaml:"serde"`
		}
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse sink config")
		}
		var err error
		if cfg.Config.Serde, err = serde.NewFromYAML(cfg.Serde); err != nil {
			return nil, err
		}
		cfg.DB = config.Storage
		return NewSink(cfg.Config), nil
	})
}

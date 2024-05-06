package ch

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	kafkapipe "github.com/egsam98/kafka-pipe"
	"github.com/egsam98/kafka-pipe/internal/registry"
)

func init() {
	registry.Register("ch.Sink", func(config registry.Config) (kafkapipe.Connector, error) {
		var cfg struct {
			SinkConfig `yaml:",inline"`
			Serde      yaml.Node `yaml:"serde"`
		}
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse sink config")
		}
		var err error
		if cfg.SinkConfig.Serde, err = kafkapipe.NewSerdeFromYAML(cfg.Serde); err != nil {
			return nil, err
		}
		cfg.DB = config.Storage
		return NewSink(cfg.SinkConfig), nil
	})
}

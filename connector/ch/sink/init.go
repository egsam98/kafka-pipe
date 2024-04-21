package sink

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"kafka-pipe/connector"
	"kafka-pipe/serde"
)

func init() {
	connector.Register("ch.Sink", func(config connector.Config) (connector.Connector, error) {
		var cfg struct {
			Config       `yaml:",inline"`
			Deserializer yaml.Node `yaml:"deserializer"`
		}
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse sink config")
		}
		var err error
		if cfg.Config.Deserializer, err = serde.NewDeserializerFromYAML("deserializer", cfg.Deserializer); err != nil {
			return nil, err
		}
		cfg.DB = config.Storage
		return NewSink(cfg.Config), nil
	})
}

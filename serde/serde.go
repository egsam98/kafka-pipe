package serde

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"kafka-pipe/internal/validate"
)

type Deserializer interface {
	Deserialize(dst any, src []byte) error
}

func NewDeserializerFromYAML(key string, value yaml.Node) (Deserializer, error) {
	var format struct {
		Value string `yaml:"format"`
	}
	if err := value.Decode(&format); err != nil {
		return nil, errors.Wrap(err, "decode serializer format")
	}

	var cfg any
	var init func() (Deserializer, error)
	switch format.Value {
	case "json":
		var jsonCfg struct {
			TimeFormat TimeFormat `yaml:"time_format" validate:"default=timestamp-milli,oneof=rfc3339 timestamp timestamp-milli"`
		}
		cfg = &jsonCfg
		init = func() (Deserializer, error) {
			return NewJSON(jsonCfg.TimeFormat), nil
		}
	case "avro":
		var avroCfg struct {
			SchemaURI string `yaml:"schema_uri" validate:"url"`
		}
		cfg = &avroCfg
		init = func() (Deserializer, error) {
			return NewAvro(avroCfg.SchemaURI)
		}
	default:
		return nil, errors.Errorf(`unknown deserializer type: %q. Possible values: [json,avro]`, format.Value)
	}

	if err := value.Decode(cfg); err != nil {
		return nil, errors.Wrap(err, "decode deserializer config")
	}
	if err := validate.Struct(cfg, key); err != nil {
		return nil, err
	}
	return init()
}

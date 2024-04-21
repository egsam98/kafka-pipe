package serde

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Deserializer interface {
	Deserialize(dst any, src []byte) error
}

func NewDeserializerFromYAML(value yaml.Node) (Deserializer, error) {
	var format struct {
		Value string `yaml:"format"`
	}
	if err := value.Decode(&format); err != nil {
		return nil, errors.Wrap(err, "decode serializer format")
	}

	var de Deserializer
	var err error
	switch format.Value {
	case "json":
		de, err = newJSONFromYAML(value)
	case "avro":
		de, err = newAvroFromYAML(value)
	default:
		return nil, errors.Errorf(`unknown deserializer type: %q. Possible values: [json,avro]`, format.Value)
	}
	if err != nil {
		var vErrs validate.Errors
		if errors.As(err, &vErrs) {
			return nil, vErrs.WithNamespace("deserializer")
		}
		return nil, err
	}
	return de, nil
}

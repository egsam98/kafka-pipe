package serde

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Serde interface {
	Deserialize(dst any, src []byte) error
}

func NewFromYAML(value yaml.Node) (Serde, error) {
	var format struct {
		Value string `yaml:"format"`
	}
	if err := value.Decode(&format); err != nil {
		return nil, errors.Wrap(err, "decode Serde format")
	}

	var serde Serde
	var err error
	switch format.Value {
	case "":
		fallthrough
	case "json":
		serde, err = newJSONFromYAML(value)
	case "avro":
		serde, err = newAvroFromYAML(value)
	default:
		return nil, errors.Errorf(`unknown Serde type: %q. Possible values: [json,avro]`, format.Value)
	}
	if err != nil {
		var vErrs validate.Errors
		if errors.As(err, &vErrs) {
			return nil, vErrs.WithNamespace("serde")
		}
		return nil, err
	}
	return serde, nil
}

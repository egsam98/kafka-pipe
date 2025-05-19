package kafkapipe

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type Serde interface {
	Deserialize(dst any, topic string, src []byte) error
	Tag() string
}

func NewSerdeFromYAML(value yaml.Node) (Serde, error) {
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
	return serde, errors.Wrap(err, "serde")
}

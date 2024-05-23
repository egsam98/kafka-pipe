package kafkapipe

import (
	"io"
	"net/http"
	"strings"

	"github.com/hamba/avro/v2"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"

	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Avro struct {
	schemas map[string]avro.Schema // Map Kafka topic to schema
}

func NewAvro(schemas map[string]string) (*Avro, error) {
	a := &Avro{schemas: make(map[string]avro.Schema)}
	for topic, schemaUrl := range schemas {
		var schema avro.Schema
		switch {
		case strings.HasPrefix(schemaUrl, "file://"):
			var err error
			if schema, err = avro.ParseFiles(schemaUrl[7:]); err != nil {
				return nil, errors.Wrap(err, "Avro: Parse file")
			}
		case strings.HasPrefix(schemaUrl, "http://"), strings.HasPrefix(schemaUrl, "https://"):
			log.Info().Msgf("Avro: Downloading schema from %s...", schemaUrl)
			res, err := http.Get(schemaUrl)
			if err != nil {
				return nil, errors.Wrap(err, "Avro: Request schema")
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				return nil, errors.Wrap(err, "Avro: read response body")
			}
			if schema, err = avro.ParseBytes(body); err != nil {
				return nil, errors.Wrap(err, "Avro: Parse bytes from response body")
			}
		default:
			return nil, errors.Errorf("Avro: Unsupported URI: %s. Supported http schemas: [file, http(-s)]", schemaUrl)
		}
		a.schemas[topic] = schema
	}

	return a, nil
}

func newAvroFromYAML(value yaml.Node) (*Avro, error) {
	var cfg struct {
		Schemas map[string]string `yaml:"schemas" validate:"required"`
	}
	if err := validate.StructFromYAML(&cfg, value); err != nil {
		return nil, err
	}
	return NewAvro(cfg.Schemas)
}

func (a *Avro) Deserialize(dst any, topic string, src []byte) error {
	schema, ok := a.schemas[topic]
	if !ok {
		return errors.Errorf("Avro: no corresponding schema for topic %q", topic)
	}
	return avro.Unmarshal(schema, src, dst)
}

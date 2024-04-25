package serde

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
	schema avro.Schema
}

func NewAvro(schemaUri string) (*Avro, error) {
	var schema avro.Schema
	switch {
	case strings.HasPrefix(schemaUri, "file://"):
		var err error
		if schema, err = avro.ParseFiles(schemaUri[7:]); err != nil {
			return nil, errors.Wrap(err, "Avro: Parse file")
		}
	case strings.HasPrefix(schemaUri, "http://"), strings.HasPrefix(schemaUri, "https://"):
		log.Info().Msgf("Avro: Downloading schema from %s...", schemaUri)
		res, err := http.Get(schemaUri)
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
		return nil, errors.Errorf("Avro: Unsupported URI: %s. Supported http schemas: [file, http(-s)]", schemaUri)
	}

	return &Avro{schema: schema}, nil
}

func newAvroFromYAML(value yaml.Node) (*Avro, error) {
	var cfg struct {
		SchemaURI string `yaml:"schema_uri" validate:"url"`
	}
	if err := validate.StructFromYAML(&cfg, value); err != nil {
		return nil, err
	}
	return NewAvro(cfg.SchemaURI)
}

func (a *Avro) Deserialize(dst any, src []byte) error {
	return avro.Unmarshal(a.schema, src, dst)
}

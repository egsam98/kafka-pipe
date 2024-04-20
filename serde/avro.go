package serde

import (
	"io"
	"net/http"
	"strings"

	"github.com/hamba/avro/v2"
	"github.com/pkg/errors"
)

type Avro struct {
	schema avro.Schema
}

func NewAvro(uri string) (*Avro, error) {
	var schema avro.Schema
	switch {
	case strings.HasPrefix(uri, "file://"):
		var err error
		if schema, err = avro.ParseFiles(uri); err != nil {
			return nil, err
		}
	case strings.HasPrefix(uri, "http://"), strings.HasPrefix(uri, "https://"):
		res, err := http.Get(uri)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		if schema, err = avro.ParseBytes(body); err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("unsupported URI: %s", uri)
	}

	return &Avro{schema: schema}, nil
}

func (a *Avro) Deserialize(dst any, src []byte) error {
	return avro.Unmarshal(a.schema, src, dst)
}

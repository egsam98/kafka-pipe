package connector

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
)

type Connector interface {
	Run(ctx context.Context) error
}

type Init func(cfg Config) (Connector, error)

type Config struct {
	Name    string
	Raw     []byte
	Storage *badger.DB
}

var connectors = make(map[string]Init)

func Register(class string, conn Init) {
	connectors[class] = conn
}

func Get(class string, cfg Config) (Connector, error) {
	init, ok := connectors[class]
	if !ok {
		return nil, errors.Errorf("unknown connector class: %s", class)
	}
	return init(cfg)
}

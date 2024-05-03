package registry

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type Init func(cfg Config) (kafkapipe.Connector, error)

type Config struct {
	Raw     []byte
	Storage *badger.DB
}

var connectors = make(map[string]Init)

func Register(class string, conn Init) {
	connectors[class] = conn
}

func Get(class string, cfg Config) (kafkapipe.Connector, error) {
	init, ok := connectors[class]
	if !ok {
		return nil, errors.Errorf("unknown connector class: %s", class)
	}
	return init(cfg)
}

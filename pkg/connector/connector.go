package connector

import (
	"context"

	"github.com/pkg/errors"

	"kafka-pipe/pkg/warden"
)

type Connector interface {
	Run(ctx context.Context) error
}

type Init func(cfg Config) (Connector, error)

type Config struct {
	Name    string
	Raw     []byte
	Storage warden.Storage
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

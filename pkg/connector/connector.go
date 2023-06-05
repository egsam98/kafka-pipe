package connector

import (
	"io"

	"github.com/pkg/errors"

	"kafka-pipe/pkg/warden"
)

type (
	Connector interface {
		io.Closer
		Run() error
	}
	Init   func(cfg Config) (Connector, error)
	Config struct {
		Name    string
		Raw     []byte
		Storage warden.Storage
	}
)

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

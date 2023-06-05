package warden

import (
	"io"

	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("not found")

type Storage interface {
	io.Closer
	Open() error
	Get(key string, dst any) error
	Set(key string, value any) error
}

package kafkapipe

import (
	"context"
)

var Version = "dev"

type Connector interface {
	Run(ctx context.Context) error
}

package badgerx

import (
	"github.com/rs/zerolog/log"
)

type Logger struct{}

func (*Logger) Errorf(s string, i ...any) {
	log.Error().Msgf("Badger: "+s, i...)
}

func (*Logger) Warningf(s string, i ...any) {
	log.Warn().Msgf("Badger: "+s, i...)
}

func (*Logger) Infof(string, ...any) {}

func (*Logger) Debugf(string, ...any) {}

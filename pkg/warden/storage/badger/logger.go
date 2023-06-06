package badger

import (
	"github.com/rs/zerolog/log"
)

type loggerAdapter struct{}

func (*loggerAdapter) Errorf(s string, i ...any) {
	log.Logger.Error().Msgf("Badger: "+s, i...)
}

func (*loggerAdapter) Warningf(s string, i ...any) {
	log.Logger.Warn().Msgf("Badger: "+s, i...)
}

func (*loggerAdapter) Infof(string, ...any) {}

func (*loggerAdapter) Debugf(string, ...any) {}

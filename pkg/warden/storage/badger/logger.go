package badger

import (
	"github.com/rs/zerolog/log"
)

const LogPrefix = "Badger: "

type loggerAdapter struct{}

func (*loggerAdapter) Errorf(s string, i ...interface{}) {
	log.Logger.Error().Msgf(LogPrefix+s, i...)
}

func (*loggerAdapter) Warningf(s string, i ...interface{}) {
	log.Logger.Warn().Msgf(LogPrefix+s, i...)
}

func (*loggerAdapter) Infof(s string, i ...interface{}) {
	log.Logger.Debug().Msgf(LogPrefix+s, i...)
}

func (*loggerAdapter) Debugf(s string, i ...interface{}) {
	log.Logger.Debug().Msgf(LogPrefix+s, i...)
}

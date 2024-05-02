package kgox

import (
	"slices"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

type logger struct {
	*zerolog.Logger
	prevErr error
	errs    chan error
}

func newLogger(base *zerolog.Logger) logger {
	return logger{
		Logger: base,
		errs:   make(chan error),
	}
}

func (l *logger) Level() kgo.LogLevel {
	return zeroLvls[l.GetLevel()]
}

func (l *logger) Log(level kgo.LogLevel, msg string, keyVals ...any) {
	if idx := slices.Index(keyVals, "err"); idx != -1 {
		err := keyVals[idx+1].(error)
		if l.prevErr == err {
			return
		}
		select {
		case l.errs <- err:
		default:
		}
		l.prevErr = err
	}
	l.Logger.WithLevel(kgoLvls[level]).Fields(keyVals).Msg("Kafka: " + msg)
}

func (l *logger) errors() <-chan error {
	return l.errs
}

var kgoLvls = map[kgo.LogLevel]zerolog.Level{
	kgo.LogLevelNone:  zerolog.NoLevel,
	kgo.LogLevelDebug: zerolog.DebugLevel,
	kgo.LogLevelInfo:  zerolog.InfoLevel,
	kgo.LogLevelWarn:  zerolog.WarnLevel,
	kgo.LogLevelError: zerolog.ErrorLevel,
}
var zeroLvls = make(map[zerolog.Level]kgo.LogLevel, len(kgoLvls))

func init() {
	for kgoLvl, zeroLvl := range kgoLvls {
		zeroLvls[zeroLvl] = kgoLvl
	}
}

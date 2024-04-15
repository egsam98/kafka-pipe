package kgox

import (
	"slices"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Logger struct {
	*zerolog.Logger
	prevErr error
	Errors  chan error
}

func NewLogger(base *zerolog.Logger) Logger {
	return Logger{
		Logger: base,
		Errors: make(chan error),
	}
}

func (l *Logger) Level() kgo.LogLevel {
	return zeroLvls[l.GetLevel()]
}

func (l *Logger) Log(level kgo.LogLevel, msg string, keyVals ...any) {
	if idx := slices.Index(keyVals, "err"); idx != -1 {
		err := keyVals[idx+1].(error)
		if l.prevErr == err {
			return
		}
		select {
		case l.Errors <- err:
		default:
		}
		l.prevErr = err
	}
	l.Logger.WithLevel(kgoLvls[level]).Fields(keyVals).Msg("Kafka: " + msg)
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

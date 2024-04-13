package kgox

import (
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Logger struct {
	zerolog.Logger
	prevErr any
}

func (l *Logger) Level() kgo.LogLevel {
	return zeroLvls[l.GetLevel()]
}

func (l *Logger) Log(level kgo.LogLevel, msg string, keyVals ...any) {
	for i := 0; i < len(keyVals); i += 2 {
		if keyVals[i] == "err" {
			err := keyVals[i+1]
			if l.prevErr == err {
				return
			}
			l.prevErr = err
		}
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

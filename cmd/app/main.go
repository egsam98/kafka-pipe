package main

import (
	"context"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"gopkg.in/yaml.v3"

	"github.com/egsam98/kafka-pipe"
	_ "github.com/egsam98/kafka-pipe/connectors/ch"
	_ "github.com/egsam98/kafka-pipe/connectors/pg"
	_ "github.com/egsam98/kafka-pipe/connectors/s3"
	"github.com/egsam98/kafka-pipe/internal/registry"

	"github.com/egsam98/kafka-pipe/internal/badgerx"
	"github.com/egsam98/kafka-pipe/internal/validate"
)

const HealthAddr = ":8081"
const DataFolder = "data"

func main() {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.TimeFieldFormat = time.RFC3339Nano
	if err := run(); err != nil {
		log.Fatal().Stack().Err(err).Str("version", kafkapipe.Version).Msg("Start Kafka Pipe")
	}
}

type PreConfig struct {
	Name  string `yaml:"name" validate:"required"`
	Class string `yaml:"class" validate:"required"`
	Log   struct {
		Pretty bool          `yaml:"pretty"`
		Level  zerolog.Level `yaml:"level"`
	} `yaml:"log"`
}

func (c *PreConfig) Parse(src []byte) error {
	if err := yaml.Unmarshal(src, c); err != nil {
		return errors.Wrap(err, "decode config class")
	}
	return validate.Struct(c)
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("YAML config is required as argument")
	}

	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		return errors.Wrap(err, "open config")
	}

	var cfg PreConfig
	if err := cfg.Parse(raw); err != nil {
		return err
	}

	var w io.Writer = os.Stdout
	if cfg.Log.Pretty {
		w = zerolog.ConsoleWriter{Out: w, TimeFormat: time.RFC3339Nano}
	}
	log.Logger = zerolog.New(w).
		Level(cfg.Log.Level).
		With().
		Timestamp().
		Logger()

	stor, err := badger.Open(badger.
		DefaultOptions(DataFolder).
		WithLogger(new(badgerx.Logger)))
	if err != nil {
		return errors.Wrapf(err, "open Badger %q", DataFolder)
	}

	conn, err := registry.Get(cfg.Class, registry.Config{
		Raw:     raw,
		Storage: stor,
	})
	if err != nil {
		return err
	}

	go httpHealth()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Info().
		Str("name", cfg.Name).
		Str("class", cfg.Class).
		Str("version", kafkapipe.Version).
		Msg("Run connector")
	if err := conn.Run(ctx); err != nil {
		return err
	}
	log.Info().Msg("Connector stopped")
	return stor.Close()
}

func httpHealth() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "OK")
	})
	if err := http.ListenAndServe(HealthAddr, nil); err != nil {
		panic(err)
	}
}

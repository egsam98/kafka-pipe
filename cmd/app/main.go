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
	"github.com/egsam98/kafka-pipe/internal/badgerx"
	"github.com/egsam98/kafka-pipe/internal/registry"
)

const HealthAddr = ":8081"
const BadgerDir = "data"

func main() {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.TimeFieldFormat = time.RFC3339Nano
	if err := run(); err != nil {
		log.Fatal().Stack().Err(err).Str("version", kafkapipe.Version).Msg("Start Kafka Pipe")
	}
}

type PreConfig struct {
	// [warden]
	// required = true
	Name string `yaml:"name"`
	// [warden]
	// required = true
	Class string `yaml:"class"`
	// [warden]
	// dive = true
	Log struct {
		Pretty bool `yaml:"pretty"`
		// [warden]
		// default = "id:github.com/rs/zerolog.InfoLevel"
		Level zerolog.Level `yaml:"level"`
	} `yaml:"log"`
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
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return errors.Wrap(err, "decode config class")
	}
	if err := cfg.Validate(); err != nil {
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

	db, err := badger.Open(badger.
		DefaultOptions(BadgerDir).
		WithSyncWrites(true).
		WithLogger(new(badgerx.Logger)))
	if err != nil {
		return errors.Wrapf(err, "open Badger %q", BadgerDir)
	}
	go badgerGc(db)

	conn, err := registry.Get(cfg.Class, registry.Config{
		Raw:     raw,
		Storage: db,
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
	return db.Close()
}

func badgerGc(db *badger.DB) {
	for range time.Tick(10 * time.Minute) {
		if err := db.RunValueLogGC(0.5); err != nil && !errors.Is(err, badger.ErrNoRewrite) {
			log.Error().Stack().Err(err).Msg("Badger: Value GC")
		}
	}
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

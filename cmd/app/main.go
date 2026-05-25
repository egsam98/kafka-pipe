package main

import (
	"context"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/egsam98/ecto"
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
	Name  string    `yaml:"name"`
	Class string    `yaml:"class"`
	Log   LogConfig `yaml:"log"`
}

type LogConfig struct {
	Pretty bool          `yaml:"pretty"`
	Level  zerolog.Level `yaml:"level"`
}

var preCfgSchema = ecto.Struct[PreConfig](ecto.M{
	"Name":  ecto.String().Required(),
	"Class": ecto.String().Required(),
	"Log": ecto.Struct[LogConfig](ecto.M{
		"Level": ecto.Atomic[zerolog.Level]().Default(zerolog.InfoLevel),
	}),
})

func run() error {
	if len(os.Args) < 2 {
		return errors.New("YAML config is required as argument")
	}

	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		return errors.Wrap(err, "open config")
	}
	raw = []byte(os.ExpandEnv(string(raw)))

	cfg, err := preCfgSchema.Cast(raw, yaml.Unmarshal)
	if err != nil {
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
	mux := http.NewServeMux() // Attach pprof routes to separate http.ServeMux
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "OK")
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{
		Addr:              HealthAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

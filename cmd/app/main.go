package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"

	"kafka-pipe/pkg/badgerx"
	"kafka-pipe/pkg/connector"
	_ "kafka-pipe/pkg/connector/pg/snapshot"
	_ "kafka-pipe/pkg/connector/pg/source"
	_ "kafka-pipe/pkg/connector/s3/sink"
)

const DataFolder = "data"

func main() {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.TimeFieldFormat = time.RFC3339Nano
	log.Logger = zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339Nano,
	}).
		With().
		Timestamp().
		Logger()
	if err := run(); err != nil {
		log.Fatal().Stack().Err(err).Msg("Start Kafka Pipe")
	}
}

type Config struct {
	Connector struct {
		Name  string `yaml:"name"`
		Class string `yaml:"class"`
	} `yaml:"connector"`
}

func (c *Config) Parse(src []byte) error {
	if err := yaml.Unmarshal(src, c); err != nil {
		return errors.Wrap(err, "decode config class")
	}
	if c.Connector.Name == "" {
		return errors.New(`"connector.name" parameter is required`)
	}
	if c.Connector.Class == "" {
		return errors.New(`"connector.class" parameter is required`)
	}
	return nil
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("YAML config is required as argument")
	}

	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		return errors.Wrap(err, "open config")
	}

	var cfg Config
	if err := cfg.Parse(data); err != nil {
		return err
	}

	stor, err := badger.Open(badger.
		DefaultOptions(DataFolder).
		WithLogger(new(badgerx.Logger)))
	if err != nil {
		return errors.Wrapf(err, "open Badger %q", DataFolder)
	}

	conn, err := connector.Get(cfg.Connector.Class, connector.Config{
		Name:    cfg.Connector.Name,
		Raw:     data,
		Storage: stor,
	})
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Info().Str("name", cfg.Connector.Name).Str("class", cfg.Connector.Class).Msg("Run connector")
	var g errgroup.Group
	g.Go(func() error { return conn.Run(ctx) })

	if err := g.Wait(); err != nil {
		return err
	}
	log.Info().Msg("Connector stopped")
	return stor.Close()
}

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"gopkg.in/yaml.v3"

	"kafka-pipe/pkg/connector"
	_ "kafka-pipe/pkg/connector/postgres/source"
	"kafka-pipe/pkg/warden/storage/badger"
)

func main() {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
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

	storage := badger.NewBadger(cfg.Connector.Name)
	if err := storage.Open(); err != nil {
		return err
	}

	conn, err := connector.Get(cfg.Connector.Class, connector.Config{
		Name:    cfg.Connector.Name,
		Raw:     data,
		Storage: storage,
	})
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Info().
		Str("name", cfg.Connector.Name).
		Str("class", cfg.Connector.Class).
		Msg("Run connector")
	go func() {
		if err := conn.Run(ctx); err != nil {
			log.Fatal().Stack().Err(err).Msg("Run connector")
		}
	}()
	//defer func() {
	//	log.Info().Msg("Closing connector...")
	//	if err := conn.Close(); err != nil {
	//		log.Error().Stack().Err(err).Msg("Close connector")
	//	}
	//}()

	//signals := make(chan os.Signal, 1)

	<-ctx.Done()
	log.Info().Msg("Shutdown")
	return nil
}

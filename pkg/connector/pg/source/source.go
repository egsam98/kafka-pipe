package source

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"kafka-pipe/pkg/connector"
	"kafka-pipe/pkg/connector/pg"
	"kafka-pipe/pkg/warden"
)

const Plugin = "pgoutput"

type Source struct {
	cfg       Config
	wg        sync.WaitGroup
	storage   warden.Storage
	prod      sarama.SyncProducer
	db        *pgconn.PgConn
	dbPool    *pgxpool.Pool
	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map
	lsn       pglogrepl.LSN
	log       zerolog.Logger
}

func NewSource(config connector.Config) (*Source, error) {
	var cfg Config
	if err := cfg.Parse(config.Raw); err != nil {
		return nil, err
	}

	s := &Source{
		cfg:       cfg,
		storage:   config.Storage,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:   pgtype.NewMap(),
	}
	pg.RegisterTypes(s.typeMap)
	s.log = log.Logger.
		With().
		Logger().
		Hook(s.lsnHook())
	return s, nil
}

func (s *Source) Run(ctx context.Context) error {
	sarAdmin, err := sarama.NewClusterAdmin(s.cfg.Kafka.Brokers, nil)
	if err != nil {
		return errors.Wrap(err, "init Kafka admin")
	}
	// Create topics if not exists
	for _, table := range s.cfg.Pg.Tables {
		topic := s.cfg.Kafka.Topic.Prefix + "." + table
		if err := sarAdmin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     s.cfg.Kafka.Topic.Partitions,
			ReplicationFactor: s.cfg.Kafka.Topic.ReplicationFactor,
			ConfigEntries: map[string]*string{
				"compression.type": &s.cfg.Kafka.Topic.CompressionType,
				"cleanup.policy":   &s.cfg.Kafka.Topic.CleanupPolicy,
			},
		}, false); err != nil {
			if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
				return errors.Wrapf(err, "create topic %q", topic)
			}
		}
	}
	sarAdmin.Close()

	// Init Kafka producer
	sarCfg := sarama.NewConfig()
	sarCfg.Net.MaxOpenRequests = 1
	sarCfg.Producer.Idempotent = true
	sarCfg.Producer.RequiredAcks = sarama.WaitForAll
	sarCfg.Producer.Compression = sarama.CompressionLZ4
	sarCfg.Producer.Return.Successes = true
	sarCfg.Producer.Retry.Max = 10
	sarCfg.Producer.Retry.Backoff = 100 * time.Millisecond
	sarCfg.Metadata.AllowAutoTopicCreation = false
	if s.prod, err = sarama.NewSyncProducer(s.cfg.Kafka.Brokers, sarCfg); err != nil {
		return errors.Wrap(err, "init Kafka producer")
	}
	if s.dbPool, err = pgxpool.New(ctx, s.cfg.Pg.Url); err != nil {
		return errors.Wrap(err, "connect to PostgreSQL")
	}

	if err := s.startReplication(ctx); err != nil {
		return err
	}

	s.wg.Add(2)
	go s.health(ctx)
	go s.listenMessages(ctx)

	// Shutdown
	<-ctx.Done()
	s.wg.Wait()

	s.log.Info().Msg("PostgreSQL: Close")
	s.db.Close(context.Background())
	s.dbPool.Close()
	s.log.Info().Msg("Kafka: Close producer")
	s.prod.Close()
	return nil
}

func (s *Source) startReplication(ctx context.Context) error {
	// Connect to PostgreSQL
	s.log.Info().Msg("PostgreSQL: Connect")
	db, err := pgconn.Connect(ctx, s.cfg.Pg.Url)
	if err != nil {
		return errors.Wrap(err, "connect to PostgreSQL server")
	}
	s.db = db
	if err := s.db.CheckConn(); err != nil {
		return errors.Wrap(err, "check connection")
	}

	// Create health check table
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id int primary key, timestamp timestamp)`, s.cfg.Pg.Health.Table)
	if _, err := s.dbPool.Exec(ctx, sql); err != nil {
		return errors.Wrap(err, sql)
	}
	s.log.Info().Msg("PostgreSQL: " + sql)

	// Create/edit publication
	tables := strings.Join(append(s.cfg.Pg.Tables, s.cfg.Pg.Health.Table), ", ")
	sql = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", s.cfg.Pg.Publication, tables)
	switch _, err := s.dbPool.Exec(ctx, sql); {
	case err == nil:
		s.log.Info().Msg("PostgreSQL: " + sql)
	case pg.Is(err, pg.DuplicateObject):
		sql := fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", s.cfg.Pg.Publication, tables)
		s.log.Info().Msg("PostgreSQL: " + sql)
		if _, err := s.dbPool.Exec(ctx, sql); err != nil {
			return errors.Wrap(err, sql)
		}
	default:
		return errors.Wrap(err, sql)
	}

	sysIdent, err := pglogrepl.IdentifySystem(ctx, s.db)
	if err != nil {
		return errors.Wrap(err, "identify system")
	}
	s.log.Info().Msgf("PostgreSQL: SystemID: %s, Timeline: %d, XLogPos: %s, DBName: %s", sysIdent.SystemID, sysIdent.Timeline, sysIdent.XLogPos, sysIdent.DBName)

	// Create replication slot if not exists
	switch _, err := pglogrepl.CreateReplicationSlot(
		ctx,
		s.db,
		s.cfg.Pg.Slot,
		Plugin,
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication},
	); {
	case err == nil:
		s.log.Info().Msgf("PostgreSQL: Create replication slot %q", s.cfg.Pg.Slot)
	case pg.Is(err, pg.DuplicateObject): // Ignore
	default:
		return errors.Wrapf(err, "create replication slot %q", s.cfg.Pg.Slot)
	}

	if err := s.storage.Get("lsn", &s.lsn); err != nil {
		if !errors.Is(err, warden.ErrNotFound) {
			return err
		}
		s.log.Warn().Msgf("PostgreSQL: LSN position is not found, using first available one from replication slot")
	}

	// Start replication
	s.log.Info().Msgf("PostgreSQL: Start logical replication")
	err = pglogrepl.StartReplication(ctx, s.db, s.cfg.Pg.Slot, s.lsn, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", s.cfg.Pg.Publication),
			"messages 'true'",
		},
		Mode: pglogrepl.LogicalReplication,
	})
	return errors.Wrap(err, "start logical replication")
}

func (s *Source) health(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.Pg.Health.Interval):
			if s.db.IsClosed() {
				continue
			}
			err := pglogrepl.SendStandbyStatusUpdate(ctx, s.db, pglogrepl.StandbyStatusUpdate{WALWritePosition: s.lsn})
			if err != nil {
				s.log.Err(err).Msg("PostgreSQL: SendStandbyStatusUpdate")
				continue
			}

			sql := fmt.Sprintf("INSERT INTO %s (id, timestamp) VALUES (0, now()) ON CONFLICT (id) DO UPDATE SET "+
				"timestamp = now()", s.cfg.Pg.Health.Table)
			if _, err := s.dbPool.Exec(ctx, sql); err != nil {
				s.log.Err(err).Msgf("PostgreSQL: Health check to %q table", s.cfg.Pg.Health.Table)
				continue
			}

			s.log.Debug().Msgf("PostgreSQL: Health check")
		}
	}
}

func (s *Source) listenMessages(ctx context.Context) {
	defer s.wg.Done()
MainLoop:
	for {
		msg, err := s.db.ReceiveMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if pgconn.Timeout(err) {
				continue
			}
			s.log.Error().Stack().Err(err).Msg("Receive message")

			for {
				err := s.startReplication(ctx)
				if err == nil {
					continue MainLoop
				}
				s.log.Error().Stack().Err(err).Msg("Start replication")

				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				}
			}
		}

		for {
			err := s.handleMessage(msg)
			if err == nil {
				break
			}
			s.log.Error().Stack().Err(err).Msg("Handle message")

			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Source) handleMessage(msg pgproto3.BackendMessage) error {
	if errMsg, ok := msg.(*pgproto3.ErrorResponse); ok {
		return errors.Errorf("Postgres WAL error: %+v", errMsg)
	}

	cd, ok := msg.(*pgproto3.CopyData)
	if !ok || cd.Data[0] != pglogrepl.XLogDataByteID {
		return nil
	}

	xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
	if err != nil {
		return errors.Wrap(err, "parse XLogData")
	}

	data, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return errors.Wrap(err, "parse WAL data")
	}

	s.log.Debug().
		Time("server_time", xld.ServerTime).
		Stringer("wal_start", xld.WALStart).
		Stringer("wal_end", xld.ServerWALEnd).
		Msgf("PostgreSQL: Received message %T", data)

	switch data := data.(type) {
	case *pglogrepl.RelationMessage:
		s.relations[data.RelationID] = data
	case *pglogrepl.InsertMessage:
		err = s.writeEvent(data.RelationID, data.Tuple.Columns, xld.WALStart)
	case *pglogrepl.UpdateMessage:
		err = s.writeEvent(data.RelationID, data.NewTuple.Columns, xld.WALStart)
	case *pglogrepl.DeleteMessage:
		if !s.cfg.Pg.SkipDelete {
			err = s.writeEvent(data.RelationID, data.OldTuple.Columns, xld.WALStart)
		}
	}
	if err != nil {
		return err
	}

	xLogPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	if err := s.storage.Set("lsn", xLogPos); err != nil {
		return err
	}
	s.lsn = xLogPos
	return nil
}

func (s *Source) writeEvent(relationID uint32, cols []*pglogrepl.TupleDataColumn, lsn pglogrepl.LSN) error {
	rel, ok := s.relations[relationID]
	if !ok {
		return errors.Errorf("unknown relation ID=%d", relationID)
	}

	// Skip health check table
	if rel.Namespace+"."+rel.RelationName == s.cfg.Pg.Health.Table {
		return nil
	}

	// Decode columns tuple into Go map
	data := make(map[string]any)
	for i, col := range cols {
		var value any
		switch col.DataType {
		case 'n': // null
		case 't', 'b':
			var format int16
			switch col.DataType {
			case 't': // text
				format = pgtype.TextFormatCode
			case 'b': // binary
				format = pgtype.BinaryFormatCode
			}

			oid := rel.Columns[i].DataType
			typ, ok := s.typeMap.TypeForOID(oid)
			if !ok {
				value = string(col.Data)
				break
			}
			var err error
			if value, err = typ.Codec.DecodeValue(s.typeMap, oid, format, col.Data); err != nil {
				return errors.Wrapf(err, "decode %q (OID=%d, format=%d)", string(col.Data), oid, format)
			}
		}

		data[rel.Columns[i].Name] = value
	}

	table := rel.Namespace + "." + rel.RelationName
	topic := s.cfg.Kafka.Topic.Prefix + "." + table
	key := sarama.StringEncoder(fmt.Sprintf(`{"id": %q}`, data["id"]))
	value, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "marshal event data")
	}

	offset, part, err := s.prod.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   key,
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("lsn"),
				Value: []byte(lsn.String()),
			},
			{
				Key:   []byte("ts_ms"),
				Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10)),
			},
			{
				Key:   []byte("table"),
				Value: []byte(table),
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, "send to Kafka")
	}

	s.log.Info().
		Int32("offset", offset).
		Int64("partition", part).
		Str("topic", topic).
		Str("message.key", string(key)).
		Msg("Kafka: Event has been published")
	return nil
}

func (s *Source) lsnHook() zerolog.HookFunc {
	return func(e *zerolog.Event, _ zerolog.Level, _ string) {
		if s.lsn == 0 {
			return
		}
		e.Stringer("lsn", s.lsn)
	}
}

package source

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"kafka-pipe/pkg/connector"
	"kafka-pipe/pkg/pg"
	"kafka-pipe/pkg/saramax"
	"kafka-pipe/pkg/warden"
)

const Plugin = "pgoutput"

type Source struct {
	cfg       Config
	wg        sync.WaitGroup
	storage   warden.Storage
	prod      sarama.SyncProducer
	conn      *pgconn.PgConn
	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map
	xLogPos   pglogrepl.LSN
}

func NewSource(config connector.Config) (*Source, error) {
	var cfg Config
	if err := cfg.Parse(config.Raw); err != nil {
		return nil, err
	}
	return &Source{
		cfg:       cfg,
		storage:   config.Storage,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:   pgtype.NewMap(),
	}, nil
}

func (s *Source) Run(ctx context.Context) error {
	// Init Kafka producer
	sarCfg := sarama.NewConfig()
	sarCfg.Net.MaxOpenRequests = 1
	sarCfg.Producer.Idempotent = true
	sarCfg.Producer.RequiredAcks = sarama.WaitForAll
	sarCfg.Producer.Compression = sarama.CompressionLZ4
	sarCfg.Producer.Return.Successes = true
	sarCfg.Producer.Retry.Max = 10
	sarCfg.Producer.Retry.Backoff = 100 * time.Millisecond
	var err error
	if s.prod, err = sarama.NewSyncProducer(s.cfg.Kafka.Brokers, sarCfg); err != nil {
		return errors.Wrap(err, "init Kafka producer")
	}

	if err := s.startReplication(ctx); err != nil {
		return err
	}

	go s.healthCheck(ctx)
	go s.listenMessages(ctx)
	<-ctx.Done()
	s.wg.Wait()

	log.Info().Msg("PostgreSQL: Close")
	if err := s.conn.Close(context.Background()); err != nil {
		log.Err(err).Msg("PostgreSQL: Close")
	}
	log.Info().Msg("Kafka: Close producer")
	if err := s.prod.Close(); err != nil {
		log.Err(err).Msg("Kafka: Close producer")
	}
	return nil
}

func (s *Source) Close() error {
	return nil
	//return s.conn.Close(context.Background())
}

func (s *Source) startReplication(ctx context.Context) error {
	log.Info().Msg("PostgreSQL: Connect")
	conn, err := pgconn.Connect(ctx, s.cfg.Pg.Url)
	if err != nil {
		return errors.Wrap(err, "connect to PostgreSQL server")
	}
	s.conn = conn
	if err := s.conn.CheckConn(); err != nil {
		return errors.Wrap(err, "check connection")
	}

	// Create publication if not exists
	sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", s.cfg.Pg.Publication, strings.Join(s.cfg.Pg.Tables, ", "))
	switch _, err := s.conn.Exec(ctx, sql).ReadAll(); {
	case err == nil:
		log.Info().Msg("PostgreSQL: " + sql)
	case pg.Is(err, pg.DuplicateObject): // Ignore
	default:
		return errors.Wrap(err, sql)
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, s.conn)
	if err != nil {
		return errors.Wrap(err, "identify system")
	}
	log.Info().Msgf("PostgreSQL: SystemID: %s, Timeline: %d, XLogPos: %s, DBName: %s", sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	// Create replication slot if not exists
	_, err = pglogrepl.CreateReplicationSlot(
		ctx,
		s.conn,
		s.cfg.Pg.Slot,
		Plugin,
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication},
	)
	switch {
	case err == nil:
		log.Info().Msgf("PostgreSQL: Create replication slot %q", s.cfg.Pg.Slot)
	case pg.Is(err, pg.DuplicateObject): // Ignore
	default:
		return errors.Wrapf(err, "create replication slot %q", s.cfg.Pg.Slot)
	}

	if err := s.storage.Get("lsn", &s.xLogPos); err != nil {
		if !errors.Is(err, warden.ErrNotFound) {
			return err
		}
		log.Warn().Msgf("PostgreSQL: LSN position is not found, using first available one from replication slot")
	}

	// Start replication
	log.Info().Stringer("lsn", s.xLogPos).Msgf("PostgreSQL: Start logical replication")
	err = pglogrepl.StartReplication(ctx, s.conn, s.cfg.Pg.Slot, s.xLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", s.cfg.Pg.Publication),
			"messages 'true'",
		},
		Mode: pglogrepl.LogicalReplication,
	})
	return errors.Wrap(err, "start logical replication")
}

func (s *Source) healthCheck(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.StandbyTimeout):
			if s.conn.IsClosed() {
				continue
			}
			err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: s.xLogPos})
			if err != nil {
				log.Error().Stack().Err(err).Stringer("lsn", s.xLogPos).Msg("SendStandbyStatusUpdate")
				continue
			}
			log.Debug().Stringer("lsn", s.xLogPos).Msgf("PostgreSQL: Sent Standby status message")
		}
	}
}

func (s *Source) listenMessages(ctx context.Context) {
MainLoop:
	for {
		msg, err := s.conn.ReceiveMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if pgconn.Timeout(err) {
				continue
			}
			log.Error().Stack().Err(err).Msg("Receive message")

			for {
				err := s.startReplication(ctx)
				if err == nil {
					continue MainLoop
				}
				log.Error().Stack().Err(err).Msg("Start replication")
				time.Sleep(5 * time.Second)
			}
		}

		for {
			err := s.handleMessage(msg)
			if err == nil {
				break
			}
			log.Error().Stack().Err(err).Msg("Handle message")
			time.Sleep(5 * time.Second)
		}
	}
}

func (s *Source) handleMessage(msg pgproto3.BackendMessage) error {
	s.wg.Add(1)
	defer s.wg.Done()

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

	log.Info().
		Time("server_time", xld.ServerTime).
		Stringer("lsn", s.xLogPos).
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
		err = s.writeEvent(data.RelationID, nil, xld.WALStart)
	}
	if err != nil {
		return err
	}

	xLogPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	if err := s.storage.Set("lsn", xLogPos); err != nil {
		return err
	}
	s.xLogPos = xLogPos
	return nil
}

type Event struct {
	Source struct {
		Table string `json:"table"`
		LSN   string `json:"lsn"`
	} `json:"source"`
	After map[string]any `json:"after"`
	TsMs  int64          `json:"ts_ms"`
}

func (s *Source) writeEvent(relationID uint32, cols []*pglogrepl.TupleDataColumn, lsn pglogrepl.LSN) error {
	rel, ok := s.relations[relationID]
	if !ok {
		return errors.Errorf("unknown relation ID=%d", relationID)
	}

	after := make(map[string]any)
	for i, col := range cols {
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			after[colName] = nil
		case 't': // text
			val, err := s.decodeTextColumnData(col.Data, rel.Columns[i].DataType)
			if err != nil {
				return errors.Wrap(err, "decoding column data")
			}
			after[colName] = val
		}
	}

	event := Event{
		Source: struct {
			Table string `json:"table"`
			LSN   string `json:"lsn"`
		}{
			Table: rel.Namespace + "." + rel.RelationName,
			LSN:   lsn.String(),
		},
		After: after,
		TsMs:  time.Now().UnixMilli(),
	}

	if _, _, err := s.prod.SendMessage(&sarama.ProducerMessage{
		Topic: fmt.Sprintf("%s.%s.%s", s.cfg.Kafka.TopicPrefix, rel.Namespace, rel.RelationName),
		Key:   sarama.StringEncoder(fmt.Sprintf(`{"id": %q"`, after["id"])),
		Value: saramax.JsonEncoder(event),
	}); err != nil {
		return errors.Wrap(err, "send to Kafka")
	}

	log.Info().Interface("event", event).Msg("Kafka: Event has been published")
	return nil
}

func (s *Source) decodeTextColumnData(data []byte, oid uint32) (interface{}, error) {
	if typ, ok := s.typeMap.TypeForOID(oid); ok && typ.Name != "uuid" && typ.Name != "numeric" {
		return typ.Codec.DecodeValue(s.typeMap, oid, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

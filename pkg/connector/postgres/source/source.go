package source

import (
	"context"
	"fmt"
	"strings"
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
	cfg             Config
	storage         warden.Storage
	prod            sarama.SyncProducer
	conn            *pgconn.PgConn
	relations       map[uint32]*pglogrepl.RelationMessage
	typeMap         *pgtype.Map
	standByDeadline time.Time
	xLogPos         pglogrepl.LSN
}

func NewSource(config connector.Config) (*Source, error) {
	var cfg Config
	if err := cfg.Parse(config.Raw); err != nil {
		return nil, err
	}
	return &Source{
		cfg:             cfg,
		storage:         config.Storage,
		relations:       make(map[uint32]*pglogrepl.RelationMessage), // TODO store
		typeMap:         pgtype.NewMap(),
		standByDeadline: time.Now().Add(cfg.StandbyTimeout),
	}, nil
}

func (s *Source) Run() error {
	ctx := context.Background()

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

	for {
		err := s.handleReplication(ctx)
		if err == nil {
			continue
		}
		log.Error().Stack().Err(err).Msg("Handle event")

		for {
			err := s.startReplication(ctx)
			if err == nil {
				break
			}
			log.Error().Stack().Err(err).Msg("PostgreSQL: Connect and start replication")
			time.Sleep(5 * time.Second)
		}
	}
}

func (s *Source) Close() error {
	return s.conn.Close(context.Background())
}

func (s *Source) startReplication(ctx context.Context) error {
	log.Info().Msg("PostgreSQL: Connect")
	var err error
	if s.conn, err = pgconn.Connect(ctx, s.cfg.Pg.Url); err != nil {
		return errors.Wrap(err, "connect to PostgreSQL server")
	}
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

	log.Info().Msgf("PostgreSQL: Start logical replication (LSN %s)", s.xLogPos)
	if err := pglogrepl.StartReplication(ctx, s.conn, s.cfg.Pg.Slot, s.xLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", s.cfg.Pg.Publication),
			"messages 'true'",
		},
		Mode: pglogrepl.LogicalReplication,
	}); err != nil {
		return errors.Wrap(err, "start logical replication")
	}

	return nil
}

func (s *Source) handleReplication(ctx context.Context) error {
	if time.Now().After(s.standByDeadline) {
		err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: s.xLogPos})
		if err != nil {
			return errors.Wrapf(err, "SendStandbyStatusUpdate (LSN %s)", s.xLogPos)
		}
		log.Debug().Msgf("PostgreSQL: Sent Standby status message (LSN %s)", s.xLogPos)
		s.standByDeadline = time.Now().Add(s.cfg.StandbyTimeout)
	}

	recCtx, cancel := context.WithDeadline(ctx, s.standByDeadline)
	rawMsg, err := s.conn.ReceiveMessage(recCtx)
	cancel()
	if err != nil {
		if pgconn.Timeout(err) {
			return nil
		}
		return errors.Wrap(err, "receive message")
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return errors.Errorf("Postgres WAL error: %+v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return nil
	}

	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return errors.Wrap(err, "ParsePrimaryKeepaliveMessage")
		}
		if pkm.ReplyRequested {
			s.standByDeadline = time.Time{}
		}
	case pglogrepl.XLogDataByteID:
		xLogPos, err := s.handleXLogData(msg.Data[1:])
		if err != nil {
			return err
		}
		if err := s.storage.Set("lsn", xLogPos); err != nil {
			return err
		}
		s.xLogPos = xLogPos
	}

	return nil
}

func (s *Source) handleXLogData(src []byte) (pglogrepl.LSN, error) {
	xld, err := pglogrepl.ParseXLogData(src)
	if err != nil {
		return 0, errors.Wrap(err, "ParseXLogData")
	}

	data, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return 0, errors.Wrap(err, "Parse logical replication message")
	}
	log.Info().Msgf("PostgreSQL: Received message %T (WALStart %s ServerWALEnd %s ServerTime %s)",
		data, xld.WALStart, xld.ServerWALEnd, xld.ServerTime)

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
		return 0, err
	}

	return xld.WALStart + pglogrepl.LSN(len(xld.WALData)), nil
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

	//for {
	_, _, err := s.prod.SendMessage(&sarama.ProducerMessage{
		Topic: fmt.Sprintf("%s.%s.%s", s.cfg.Kafka.TopicPrefix, rel.Namespace, rel.RelationName),
		Key:   sarama.StringEncoder(fmt.Sprintf(`{"id": %q"`, after["id"])),
		Value: saramax.JsonEncoder(event),
	})
	if err != nil {
		return errors.Wrap(err, "send event to Kafka")
	}
	//if err == nil {
	//	break
	//}
	//log.Error().Stack().Err(err).Msg("Write to Kafka")
	//time.Sleep(5 * time.Second)
	//}

	return nil
}

func (s *Source) decodeTextColumnData(data []byte, oid uint32) (interface{}, error) {
	if typ, ok := s.typeMap.TypeForOID(oid); ok && typ.Name != "uuid" && typ.Name != "numeric" {
		return typ.Codec.DecodeValue(s.typeMap, oid, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

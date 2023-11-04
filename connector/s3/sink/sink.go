package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-pipe/connector"
	"kafka-pipe/connector/s3"
)

type Sink struct {
	wg    sync.WaitGroup
	name  string
	cfg   Config
	kafka *kgo.Client
	s3    *minio.Client
}

func NewSink(config connector.Config) (*Sink, error) {
	var cfg Config
	if err := cfg.Parse(config.Raw); err != nil {
		return nil, err
	}

	return &Sink{
		name: config.Name,
		cfg:  cfg,
	}, nil
}

func (s *Sink) Run(ctx context.Context) error {
	// Init consumer group
	var err error
	if s.kafka, err = kgo.NewClient(
		kgo.SeedBrokers(s.cfg.Kafka.Brokers...),
		kgo.ConsumeTopics(s.cfg.Kafka.Topics...),
		kgo.ConsumerGroup(s.name),
		kgo.AutoCommitMarks(),
	); err != nil {
		return errors.Wrap(err, "init Kafka consumer group")
	}
	if err := s.kafka.Ping(ctx); err != nil {
		return errors.Wrap(err, "ping Kafka brokers")
	}

	// Init MinIO client - S3 compatible storage
	if s.s3, err = minio.New(s.cfg.S3.URL, &minio.Options{
		Creds:  credentials.NewStaticV4(s.cfg.S3.AccessKeyID, s.cfg.S3.SecretKeyAccess, ""),
		Secure: s.cfg.S3.SSL,
	}); err != nil {
		return errors.Wrap(err, "init MinIO client")
	}
	if _, err := s.s3.ListBuckets(ctx); err != nil {
		return errors.Wrap(err, "ping S3")
	}

	records, err := s.poll(ctx)
	if err != nil {
		return err
	}

	s.wg.Add(len(records))
	for _, recordCh := range records {
		go s.listenRecords(ctx, recordCh)
	}

	<-ctx.Done()
	s.wg.Wait()
	log.Info().Msg("Kafka: Close consumer group")
	s.kafka.Close()
	return nil
}

func (s *Sink) poll(ctx context.Context) (map[string]chan *kgo.Record, error) {
	records := make(map[string]chan *kgo.Record)
	for _, topic := range s.cfg.Kafka.Topics {
		records[topic] = make(chan *kgo.Record)
	}

	log.Info().Msgf("Kafka: Listening to %v", s.cfg.Kafka.Topics)

	go func() {
		defer func() {
			for _, records := range records {
				close(records)
			}
		}()

		for {
			pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			fes := s.kafka.PollFetches(pollCtx)
			cancel()
			if err := fes.Err(); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					log.Err(err).Msgf("Kafka: Poll fetches")
				}
			}

			fes.EachRecord(func(rec *kgo.Record) {
				records[rec.Topic] <- rec
			})
		}
	}()

	return records, nil
}

func (s *Sink) listenRecords(ctx context.Context, records <-chan *kgo.Record) {
	defer s.wg.Done()

	parts := make(map[string][]*kgo.Record)
	offsets := make(map[int32]int64)
	var overflowKey string

	ticker := time.NewTicker(s.cfg.S3.Flush.Timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case rec, ok := <-records:
			if !ok {
				return
			}
			if offset, ok := offsets[rec.Partition]; ok && rec.Offset <= offset {
				continue
			}

			key := rec.Timestamp.UTC().Round(s.cfg.S3.Flush.Timeout).Format(s3.TimeFmt)
			parts[key] = append(parts[key], rec)
			offsets[rec.Partition] = rec.Offset
			if len(parts[key]) < s.cfg.S3.Flush.Size {
				continue
			}
			overflowKey = key
		}

		for key, part := range parts {
			if overflowKey != "" && key != overflowKey {
				continue
			}

			filename := fmt.Sprintf("%s/%s.json.gz", part[0].Topic, key)

			for {
				err := s.uploadToS3(filename, part)
				if err == nil {
					break
				}

				log.Error().Stack().Err(err).Msgf("S3: Upload to %q", filename)
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				}
			}

			s.kafka.MarkCommitRecords(part...)
			delete(parts, key)
			log.Info().Int("count", len(part)).Msgf("S3: Messages have been uploaded to %q", filename)

			if overflowKey != "" {
				break
			}
		}

		overflowKey = ""
	}
}

func (s *Sink) uploadToS3(filename string, records []*kgo.Record) error {
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	_, _ = gzw.Write([]byte{'['})
	for i, record := range records {
		b, err := json.Marshal(s3.NewRecord(record))
		if err != nil {
			return errors.Wrap(err, "marshal Kafka record's data")
		}

		_, _ = gzw.Write(b)
		var tail byte = ','
		if i == len(records)-1 {
			tail = ']'
		}
		_, _ = gzw.Write([]byte{tail})
	}
	if err := gzw.Close(); err != nil {
		return errors.Wrap(err, "gzip encode")
	}

	_, err := s.s3.PutObject(context.Background(), s.cfg.S3.Bucket, filename, &buf, int64(buf.Len()), minio.PutObjectOptions{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
	})
	return errors.Wrap(err, "put object")
}

package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/egsam98/kafka-pipe/internal/kgox"
	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Sink struct {
	cfg        SinkConfig
	consumPool kgox.ConsumerPool
	s3         *minio.Client
}

func NewSink(cfg SinkConfig) (*Sink, error) {
	return &Sink{cfg: cfg}, nil
}

func (s *Sink) Run(ctx context.Context) error {
	if err := validate.Struct(&s.cfg); err != nil {
		return err
	}

	// Init consumer group
	var err error
	if s.consumPool, err = kgox.NewConsumerPool(s.cfg.Kafka); err != nil {
		return err
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

	log.Info().Msg("Kafka: Listening to topics...")
	s.consumPool.Listen(ctx, s.s3Write)
	log.Info().Msg("Kafka: Disconnect")
	s.consumPool.Close()
	return nil
}

func (s *Sink) s3Write(ctx context.Context, fetches kgo.Fetches) error {
	rawFiles := make(map[int64]encoder)
	var recordsNum int
	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()
		recordsNum++

		timeUnix := rec.Timestamp.Round(s.cfg.S3.Period).Unix()
		enc, ok := rawFiles[timeUnix]
		if !ok {
			enc = newEncoder()
			rawFiles[timeUnix] = enc
		}

		enc.WriteVal(newRow(rec))
		enc.WriteRaw("\n")
		if err := enc.Flush(); err != nil {
			return err
		}
	}

	for _, file := range rawFiles {
		if err := file.close(); err != nil {
			return err
		}
	}

	var g errgroup.Group
	for timeUnix, file := range rawFiles {
		filename := fmt.Sprintf("%s/%d.gz", fetches[0].Topics[0].Topic, timeUnix)
		reader, n := file.buffered()
		g.Go(func() error {
			_, err := s.s3.PutObject(ctx, s.cfg.S3.Bucket, filename, reader, n, minio.PutObjectOptions{
				ContentType:     "application/json",
				ContentEncoding: "gzip",
			})
			return errors.Wrap(err, "put object")
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	log.Info().Msgf("S3: %d messages have been uploaded", recordsNum)
	return nil
}

type encoder struct {
	*jsoniter.Stream
	buf *bytes.Buffer
	gzw *gzip.Writer
}

func newEncoder() encoder {
	var buf bytes.Buffer
	gzw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	stream := jsoniter.ConfigDefault.BorrowStream(gzw)
	return encoder{
		Stream: stream,
		buf:    &buf,
		gzw:    gzw,
	}
}

func (e *encoder) buffered() (io.Reader, int64) {
	return e.buf, int64(e.buf.Len())
}

func (e *encoder) close() error {
	jsoniter.ConfigDefault.ReturnStream(e.Stream)
	return e.gzw.Close()
}

type row struct {
	Headers []header `json:"headers"`
	Key     string   `json:"key"`
	Value   string   `json:"value"`
}

func newRow(rec *kgo.Record) row {
	r := row{
		Headers: make([]header, len(rec.Headers)),
		Key:     string(rec.Key),
		Value:   string(rec.Value),
	}
	for i, h := range rec.Headers {
		r.Headers[i] = header{Key: h.Key, Value: string(h.Value)}
	}
	return r
}

type header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

//func (s *Sink) poll(ctx context.Context) (map[string]chan *kgo.Record, error) {
//	records := make(map[string]chan *kgo.Record)
//	for _, topic := range s.cfg.Kafka.Topics {
//		records[topic] = make(chan *kgo.Record)
//	}
//
//	log.Info().Msgf("Kafka: Listening to %v", s.cfg.Kafka.Topics)
//
//	go func() {
//		defer func() {
//			for _, records := range records {
//				close(records)
//			}
//		}()
//
//		for {
//			pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
//			fes := s.kafka.PollFetches(pollCtx)
//			cancel()
//			if err := fes.Err(); err != nil {
//				if errors.Is(err, context.Canceled) {
//					return
//				}
//				if !errors.Is(err, context.DeadlineExceeded) {
//					log.Err(err).Msgf("Kafka: Poll fetches")
//				}
//			}
//
//			fes.EachRecord(func(rec *kgo.Record) {
//				records[rec.Topic] <- rec
//			})
//		}
//	}()
//
//	return records, nil
//}
//
//func (s *Sink) listenRecords(ctx context.Context, records <-chan *kgo.Record) {
//	defer s.wg.Done()
//
//	parts := make(map[string][]*kgo.Record)
//	offsets := make(map[int32]int64)
//	var overflowKey string
//
//	ticker := time.NewTicker(s.cfg.S3.Flush.Timeout)
//	defer ticker.Stop()
//
//	for {
//		select {
//		case <-ticker.C:
//		case rec, ok := <-records:
//			if !ok {
//				return
//			}
//			if offset, ok := offsets[rec.Partition]; ok && rec.Offset <= offset {
//				continue
//			}
//
//			key := rec.Timestamp.UTC().Round(s.cfg.S3.Flush.Timeout).Format(TimeFmt)
//			parts[key] = append(parts[key], rec)
//			offsets[rec.Partition] = rec.Offset
//			if len(parts[key]) < s.cfg.S3.Flush.Size {
//				continue
//			}
//			overflowKey = key
//		}
//
//		for key, part := range parts {
//			if overflowKey != "" && key != overflowKey {
//				continue
//			}
//
//			filename := fmt.Sprintf("%s/%s.json.gz", part[0].Topic, key)
//
//			for {
//				err := s.uploadToS3(filename, part)
//				if err == nil {
//					break
//				}
//
//				log.Error().Stack().Err(err).Msgf("S3: Upload to %q", filename)
//				select {
//				case <-time.After(5 * time.Second):
//				case <-ctx.Done():
//					return
//				}
//			}
//
//			s.kafka.MarkCommitRecords(part...)
//			delete(parts, key)
//			log.Info().Int("count", len(part)).Msgf("S3: Messages have been uploaded to %q", filename)
//
//			if overflowKey != "" {
//				break
//			}
//		}
//
//		overflowKey = ""
//	}
//}
//
//func (s *Sink) uploadToS3(filename string, records []*kgo.Record) error {
//	var buf bytes.Buffer
//	gzw := gzip.NewWriter(&buf)
//	_, _ = gzw.Write([]byte{'['})
//	for i, record := range records {
//		b, err := json.Marshal(newRecord(record))
//		if err != nil {
//			return errors.Wrap(err, "marshal Kafka record's data")
//		}
//
//		_, _ = gzw.Write(b)
//		var tail byte = ','
//		if i == len(records)-1 {
//			tail = ']'
//		}
//		_, _ = gzw.Write([]byte{tail})
//	}
//	if err := gzw.Close(); err != nil {
//		return errors.Wrap(err, "gzip encode")
//	}
//
//	_, err := s.s3.PutObject(context.Background(), s.cfg.S3.Bucket, filename, &buf, int64(buf.Len()), minio.PutObjectOptions{
//		ContentType:     "application/json",
//		ContentEncoding: "gzip",
//	})
//	return errors.Wrap(err, "put object")
//}

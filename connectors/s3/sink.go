package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"
	"unsafe"

	"github.com/bwmarrin/snowflake"
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
	snowflake  *snowflake.Node
}

func NewSink(cfg SinkConfig) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) Run(ctx context.Context) error {
	if err := validate.Struct(&s.cfg); err != nil {
		return err
	}
	var err error
	if s.snowflake, err = snowflake.NewNode(0); err != nil {
		return err
	}

	// Init consumer group
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

	var wg sync.WaitGroup
	for _, topic := range s.cfg.Kafka.Topics {
		wg.Add(1)
		go s.listenMerge(ctx, topic, &wg)
	}

	s.consumPool.Listen(ctx, s.write)
	wg.Wait()
	log.Info().Msg("Kafka: Disconnect")
	s.consumPool.Close()
	return nil
}

func (s *Sink) write(_ context.Context, fetches kgo.Fetches) error {
	rawFiles := make(map[string]encoder)
	var recordsNum int
	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()
		recordsNum++

		timeStr := rec.Timestamp.UTC().Truncate(s.cfg.DataPeriod).Format(time.DateTime)
		enc, ok := rawFiles[timeStr]
		if !ok {
			enc = newEncoder()
			rawFiles[timeStr] = enc
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
	for timeStr, file := range rawFiles {
		key := fmt.Sprintf("%s/%s/%d.gz",
			fetches[0].Topics[0].Topic,
			timeStr,
			s.snowflake.Generate(),
		)
		reader, n := file.buffered()
		g.Go(func() error {
			_, err := s.s3.PutObject(context.Background(), s.cfg.S3.Bucket, key, reader, n, minio.PutObjectOptions{
				ContentType:     "application/gzip",
				ContentEncoding: "gzip",
			})
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	log.Info().Msgf("S3: %d messages have been uploaded", recordsNum)
	return nil
}

func (s *Sink) listenMerge(ctx context.Context, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.MergeTick):
			for {
				if err := s.merge(topic); err != nil && !errors.Is(err, context.Canceled) {
					log.Err(err).Msgf("S3: Merge")
				}
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (s *Sink) merge(topic string) error {
	type group struct {
		size int64
		keys []string
	}

	//language=goregexp
	regex, err := regexp.Compile(fmt.Sprintf(`(%s/\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d)[/_]\w+\.gz`, topic))
	if err != nil {
		return err
	}

	ctx := context.Background()
	groups := make(map[string]group)
	for obj := range s.s3.ListObjects(ctx, s.cfg.S3.Bucket, minio.ListObjectsOptions{
		Prefix:     topic,
		Recursive:  true,
		StartAfter: topic + "/" + time.Now().UTC().Add(-s.cfg.MergeOffset).Format(time.DateTime),
	}) {
		if obj.Err != nil {
			return obj.Err
		}
		if obj.Size == 0 {
			continue
		}
		parts := regex.FindStringSubmatch(obj.Key)
		if len(parts) < 2 {
			continue
		}
		prefix := parts[1]
		g := groups[prefix]
		g.keys = append(g.keys, obj.Key)
		g.size += obj.Size
		groups[prefix] = g
	}

	var updPrefixes []string
	for prefix, group := range groups {
		if len(group.keys) < 2 {
			continue
		}

		readers := make([]io.Reader, len(group.keys))
		for i, key := range group.keys {
			obj, err := s.s3.GetObject(ctx, s.cfg.S3.Bucket, key, minio.GetObjectOptions{})
			if err != nil {
				return err
			}
			readers[i] = obj
		}

		if _, err := s.s3.PutObject(
			ctx,
			s.cfg.S3.Bucket,
			fmt.Sprintf("%s/%d.gz", prefix, s.snowflake.Generate()),
			io.MultiReader(readers...),
			group.size,
			minio.PutObjectOptions{
				ContentType:           "application/gzip",
				ContentEncoding:       "gzip",
				ConcurrentStreamParts: true,
				NumThreads:            5,
			},
		); err != nil {
			return err
		}

		var g errgroup.Group
		for _, key := range group.keys {
			g.Go(func() error {
				return s.s3.RemoveObject(ctx, s.cfg.S3.Bucket, key, minio.RemoveObjectOptions{})
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		updPrefixes = append(updPrefixes, prefix)
	}

	if len(updPrefixes) > 0 {
		log.Info().Strs("prefixes", updPrefixes).Msg("S3: Keys have been merged")
	}
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
	Partition int32    `json:"partition"`
	Headers   []header `json:"headers"`
	Key       string   `json:"key"`
	Value     string   `json:"value"`
}

func newRow(rec *kgo.Record) row {
	r := row{
		Partition: rec.Partition,
		Headers:   make([]header, len(rec.Headers)),
	}
	if n := len(rec.Key); n > 0 {
		r.Key = unsafe.String(&rec.Key[0], n)
	}
	if n := len(rec.Value); n > 0 {
		r.Value = unsafe.String(&rec.Value[0], n)
	}
	for i, h := range rec.Headers {
		hr := header{Key: h.Key}
		if n := len(h.Value); n > 0 {
			hr.Value = unsafe.String(&h.Value[0], n)
		}
		r.Headers[i] = hr
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

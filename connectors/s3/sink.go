package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/egsam98/kafka-pipe/internal/kgox"
	"github.com/egsam98/kafka-pipe/internal/syncx"
	"github.com/egsam98/kafka-pipe/internal/validate"
)

const maxMergeKeySize = 5 * 1024 * 1024 // 5MB
var regexKeySuffix = regexp.MustCompile(`/\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d/\d+/(\d+)-(\d+).gz$`)

type Sink struct {
	cfg        SinkConfig
	s3         *minio.Client
	deleteKeys *syncx.Queue[string]
}

func NewSink(cfg SinkConfig) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) Run(ctx context.Context) error {
	if err := validate.Struct(&s.cfg); err != nil {
		return err
	}

	// Init consumer pool
	consumPool, err := kgox.NewConsumerPool(s.cfg.Kafka)
	if err != nil {
		return err
	}

	// Init MinIO client - S3 compatible storage
	if s.s3, err = minio.New(s.cfg.S3.URL, &minio.Options{
		Creds:  credentials.NewStaticV4(s.cfg.S3.AccessKeyID, s.cfg.S3.SecretKeyAccess, ""),
		Secure: s.cfg.S3.SSL,
	}); err != nil {
		return errors.Wrap(err, "init S3 client")
	}
	if _, err := s.s3.ListBuckets(ctx); err != nil {
		return errors.Wrap(err, "ping S3")
	}

	if s.deleteKeys, err = syncx.NewQueue[string](s.cfg.Name, s.cfg.DB); err != nil {
		return err
	}

	go s.deleteKeys.Listen(ctx, func(key string) error {
		return s.s3.RemoveObject(ctx, s.cfg.S3.Bucket, key, minio.RemoveObjectOptions{})
	})

	log.Info().Msg("Kafka: Listening to topics...")
	consumPool.Listen(ctx, func(_ context.Context, fetches kgo.Fetches) error {
		return s.listen(fetches)
	})
	log.Info().Msg("Kafka: Disconnect")
	consumPool.Close()
	return s.deleteKeys.Close()
}

// listen handles Kafka messages from common topic
func (s *Sink) listen(fetches kgo.Fetches) error {
	encoders := make(map[string]*encoder) // Each encoder is unique for topic+truncated time+partition
	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()

		prefix := fmt.Sprintf("%s/%s/%d",
			rec.Topic,
			rec.Timestamp.UTC().Truncate(s.cfg.DataPeriod).Format(time.DateTime),
			rec.Partition,
		)
		enc, ok := encoders[prefix]
		if !ok {
			enc = newEncoder()
			encoders[prefix] = enc
		}

		if enc.minOffset < 0 {
			enc.minOffset = rec.Offset
		}
		enc.maxOffset = rec.Offset

		enc.WriteVal(newRow(rec))
		enc.WriteRaw("\n")
		if err := enc.Flush(); err != nil {
			return errors.Wrapf(err, "encode Kafka record: %+v", *rec)
		}
	}

	// Close encoders
	for _, enc := range encoders {
		if err := enc.close(); err != nil {
			return err
		}
	}

	// Upload data to S3 for every encoder
	var g errgroup.Group
	for prefix, enc := range encoders {
		g.Go(func() error { return s.s3Write(prefix, enc) })
	}
	return g.Wait()
}

// s3Write uploads buffered data in encoder to S3 storage with `{prefix}/{minOffset}-{maxOffset}.gz` key.
// The prefix is `{topic}{truncated time}{partition}`.
// The data might be merged with the latest object if its size doesn't exceed maxMergeKeySize
func (s *Sink) s3Write(prefix string, enc *encoder) error {
	// Find the latest object
	ctx, cancel := context.WithCancel(context.Background()) // To prevent memory leak after breaking channel consumption
	defer cancel()
	var latestObj minio.ObjectInfo
	for obj := range s.s3.ListObjects(ctx, s.cfg.S3.Bucket, minio.ListObjectsOptions{Prefix: prefix + "/"}) {
		if obj.Err != nil {
			return errors.Wrapf(obj.Err, `S3: list objects by prefix "%s/"`, prefix)
		}
		latestObj = obj
	}

	key := fmt.Sprintf("%s/%018d-%018d.gz", prefix, enc.minOffset, enc.maxOffset)
	reader, n := enc.buffered()
	var merge bool

	if latestObj.Key != "" {
		parts := regexKeySuffix.FindStringSubmatch(latestObj.Key)
		if len(parts) < 3 {
			return errors.Errorf("S3: invalid key: %q", latestObj.Key)
		}
		latestMinOffset := parts[1]
		if latestMaxOffset, _ := strconv.ParseInt(parts[2], 10, 64); latestMaxOffset >= enc.maxOffset {
			log.Warn().Msgf("S3: %d (latest max offset) >= %d (current max offset), skipping key %q",
				latestMaxOffset, enc.maxOffset, key)
			return nil
		}

		// Merge new object with the latest one
		if latestObj.Size <= maxMergeKeySize {
			merge = true

			obj, err := s.s3.GetObject(ctx, s.cfg.S3.Bucket, latestObj.Key, minio.GetObjectOptions{})
			if err != nil {
				return errors.Wrapf(err, "S3: get %q", latestObj.Key)
			}

			key = fmt.Sprintf("%s/%s-%018d.gz", prefix, latestMinOffset, enc.maxOffset)
			reader = io.MultiReader(obj, reader)
			n += latestObj.Size
		}
	}

	start := time.Now()
	if _, err := s.s3.PutObject(ctx, s.cfg.S3.Bucket, key, reader, n, minio.PutObjectOptions{
		ContentType:     "application/gzip",
		ContentEncoding: "gzip",
	}); err != nil {
		return errors.Wrapf(err, "S3: Put %q", key)
	}
	uploadDur := time.Since(start)

	if merge {
		if err := s.s3.RemoveObject(ctx, s.cfg.S3.Bucket, latestObj.Key, minio.RemoveObjectOptions{}); err != nil {
			if err := s.deleteKeys.Push(latestObj.Key); err != nil {
				return err
			}
		}
	}

	event := log.Info().
		Int64("messages", enc.maxOffset-enc.minOffset+1).
		Str("key", key).
		Int64("size_b", n).
		Dur("duration_ms", uploadDur)
	if merge {
		event = event.Str("merged_with", latestObj.Key)
	}
	event.Msgf("S3: Uploaded")
	return nil
}

// encoder writes gzip-compressed JSON data. It also holds accumulated buffer and min/max offsets
type encoder struct {
	*jsoniter.Stream
	buf                  *bytes.Buffer
	gzw                  *gzip.Writer
	minOffset, maxOffset int64
}

func newEncoder() *encoder {
	var buf bytes.Buffer
	gzw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	stream := jsoniter.ConfigDefault.BorrowStream(gzw)
	return &encoder{
		Stream:    stream,
		buf:       &buf,
		gzw:       gzw,
		minOffset: -1,
	}
}

func (e *encoder) buffered() (io.Reader, int64) {
	return e.buf, int64(e.buf.Len())
}

func (e *encoder) close() error {
	jsoniter.ConfigDefault.ReturnStream(e.Stream)
	return errors.WithStack(e.gzw.Close())
}

// row represents a line of encoded data in S3
type row struct {
	Offset  int64    `json:"offset"`
	Key     string   `json:"key"`
	Value   string   `json:"value"`
	Headers []header `json:"headers"`
}

func newRow(rec *kgo.Record) row {
	r := row{
		Offset:  rec.Offset,
		Headers: make([]header, len(rec.Headers)),
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

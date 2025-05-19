package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/egsam98/kafka-pipe/internal/badgerx"
	"github.com/egsam98/kafka-pipe/internal/kgox"
)

const maxMergeKeySize = 5 * 1024 * 1024 // 5MB
const metaCount = "X-Amz-Meta-Count"

type Sink struct {
	cfg        SinkConfig
	s3         *minio.Client
	deleteKeys *badgerx.Queue[string]
}

func NewSink(cfg SinkConfig) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) Run(ctx context.Context) error {
	if err := s.cfg.Validate(); err != nil {
		return err
	}

	// Init consumer pool
	consumPool, err := kgox.NewConsumerPool(s.cfg.Kafka)
	if err != nil {
		return err
	}

	// Init MinIO client - S3 compatible storage
	if s.s3, err = minio.New(s.cfg.S3.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s.cfg.S3.ID, s.cfg.S3.Secret, ""),
		Secure: s.cfg.S3.SSL,
	}); err != nil {
		return errors.Wrap(err, "init S3 client")
	}
	if _, err := s.s3.ListBuckets(ctx); err != nil {
		return errors.Wrap(err, "ping S3")
	}

	if s.deleteKeys, err = badgerx.NewQueue[string](s.cfg.Name, s.cfg.DB); err != nil {
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
	encoders := make(map[string]*encoder) // Each encoder is unique for topic+truncated datetime+partition
	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()

		prefix := fmt.Sprintf("%s/%s/%d",
			rec.Topic,
			rec.Timestamp.UTC().Truncate(s.cfg.GroupTimeInterval).Format(time.DateTime),
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

		if err := enc.encode(rec); err != nil {
			return err
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
// The data might be merged with previous object if its size doesn't exceed maxMergeKeySize
func (s *Sink) s3Write(prefix string, enc *encoder) error {
	// Find previous object
	ctx, cancel := context.WithCancel(context.Background()) // To prevent memory leak after breaking channel consumption
	defer cancel()
	var prevObj minio.ObjectInfo
	for obj := range s.s3.ListObjects(ctx, s.cfg.S3.Bucket, minio.ListObjectsOptions{
		Prefix:       prefix + "/",
		WithMetadata: true,
	}) {
		if obj.Err != nil {
			return errors.Wrapf(obj.Err, `S3: list objects by prefix "%s/"`, prefix)
		}
		prevObj = obj
	}

	key := fmt.Sprintf("%s/%018d-%018d.gz", prefix, enc.minOffset, enc.maxOffset)
	var reader io.Reader = enc.buf
	size := int64(enc.buf.Len())
	count := enc.count
	var merge bool

	if prevObj.Key != "" {
		segments, err := newKeySegments(prevObj.Key)
		if err != nil {
			return err
		}
		prevMinOffset, err := segments.minOffset()
		if err != nil {
			return err
		}
		prevMaxOffset, err := segments.maxOffset()
		if err != nil {
			return err
		}
		if prevMaxOffset >= enc.maxOffset {
			log.Warn().Msgf("S3: %d (prev max offset) >= %d (current max offset), skipping key %q",
				prevMaxOffset, enc.maxOffset, key)
			return nil
		}

		// Merge new object with the previous one
		if prevObj.Size <= maxMergeKeySize {
			merge = true

			obj, err := s.s3.GetObject(ctx, s.cfg.S3.Bucket, prevObj.Key, minio.GetObjectOptions{})
			if err != nil {
				return errors.Wrapf(err, "S3: get %q", prevObj.Key)
			}
			defer obj.Close()

			key = fmt.Sprintf("%s/%018d-%018d.gz", prefix, prevMinOffset, enc.maxOffset)
			reader = io.MultiReader(obj, reader)
			size += prevObj.Size

			prevCountStr, ok := prevObj.UserMetadata[metaCount]
			// If S3 storage doesn't return meta from ListObjects call additional HEAD request for prev object
			if !ok {
				stat, err := s.s3.StatObject(ctx, s.cfg.S3.Bucket, prevObj.Key, minio.StatObjectOptions{})
				if err != nil {
					return errors.Wrapf(err, "S3: stat previous object %s", prevObj.Key)
				}
				prevCountStr = stat.Metadata.Get(metaCount)
			}
			prevCount, err := strconv.Atoi(prevCountStr)
			if err != nil {
				return errors.Wrapf(err, "S3: invalid %s metadata in previous object %s", metaCount, prevObj.Key)
			}
			count += prevCount
		}
	}

	start := time.Now()
	if _, err := s.s3.PutObject(ctx, s.cfg.S3.Bucket, key, reader, size, minio.PutObjectOptions{
		ContentType:     "application/gzip",
		ContentEncoding: "gzip",
		UserMetadata:    map[string]string{metaCount: strconv.Itoa(count)},
	}); err != nil {
		return errors.Wrapf(err, "S3: Put %q", key)
	}
	uploadDur := time.Since(start)

	if merge {
		if err := s.s3.RemoveObject(ctx, s.cfg.S3.Bucket, prevObj.Key, minio.RemoveObjectOptions{}); err != nil {
			if err := s.deleteKeys.Push(prevObj.Key); err != nil {
				return err
			}
		}
	}

	event := log.Info().
		Int64("messages", enc.maxOffset-enc.minOffset+1).
		Str("key", key).
		Int64("size_b", size).
		Dur("duration_ms", uploadDur)
	if merge {
		event = event.Str("merged_with", prevObj.Key)
	}
	event.Msgf("S3: Uploaded")
	return nil
}

// encoder writes gzip-compressed JSON data. It also holds accumulated buffer and min/max offsets
type encoder struct {
	count                int
	minOffset, maxOffset int64
	stream               *jsoniter.Stream
	buf                  *bytes.Buffer
	gzw                  *gzip.Writer
}

func newEncoder() *encoder {
	var buf bytes.Buffer
	gzw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	stream := jsoniter.ConfigDefault.BorrowStream(gzw)
	return &encoder{
		stream:    stream,
		buf:       &buf,
		gzw:       gzw,
		minOffset: -1,
	}
}

func (e *encoder) encode(rec *kgo.Record) error {
	e.stream.WriteVal(newJsonRow(rec))
	e.stream.WriteRaw("\n")
	if err := e.stream.Flush(); err != nil {
		return errors.Wrapf(err, "encode Kafka record: %+v", *rec)
	}
	e.count++
	return nil
}

func (e *encoder) close() error {
	jsoniter.ConfigDefault.ReturnStream(e.stream)
	return errors.WithStack(e.gzw.Close())
}

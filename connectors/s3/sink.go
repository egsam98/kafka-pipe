package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"regexp"
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

const maxMergeKeySize = 10 * 1024 * 1024 // 10MB
var regexKeySuffix = regexp.MustCompile(`/\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d/\d+/(\d+)-(\d+).gz$`)

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
	s.consumPool.Listen(ctx, func(_ context.Context, fetches kgo.Fetches) error {
		return s.listen(fetches)
	})
	log.Info().Msg("Kafka: Disconnect")
	s.consumPool.Close()
	return nil
}

func (s *Sink) listen(fetches kgo.Fetches) error {
	encoders := make(map[string]*encoder)
	var recordsNum int
	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()
		recordsNum++

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
			return err
		}
	}

	for _, enc := range encoders {
		if err := enc.close(); err != nil {
			return err
		}
	}

	var g errgroup.Group
	for prefix, enc := range encoders {
		g.Go(func() error { return s.s3Write(prefix, enc) })
	}
	if err := g.Wait(); err != nil {
		return err
	}

	log.Info().Msgf("S3: %d messages have been uploaded", recordsNum)
	return nil
}

func (s *Sink) s3Write(prefix string, enc *encoder) error {
	ctx, cancel := context.WithCancel(context.Background()) // To prevent memory leak after breaking channel
	defer cancel()
	var latestObj minio.ObjectInfo
	for obj := range s.s3.ListObjects(ctx, s.cfg.S3.Bucket, minio.ListObjectsOptions{Prefix: prefix + "/"}) {
		if obj.Err != nil {
			return obj.Err
		}
		latestObj = obj
	}

	fmt.Println("LAST KEY", latestObj.Key)
	key := fmt.Sprintf("%s/%018d-%018d.gz", prefix, enc.minOffset, enc.maxOffset)
	reader, n := enc.buffered()
	var removeLatest bool

	if latestObj.Key != "" {
		if latestObj.Key == key {
			return nil
		}
		if latestObj.Size <= maxMergeKeySize {
			removeLatest = true

			obj, err := s.s3.GetObject(ctx, s.cfg.S3.Bucket, latestObj.Key, minio.GetObjectOptions{})
			if err != nil {
				return err
			}
			parts := regexKeySuffix.FindStringSubmatch(latestObj.Key)
			if len(parts) < 2 {
				return errors.Errorf("invalid key: %q", latestObj.Key)
			}

			key = fmt.Sprintf("%s/%s-%018d.gz", prefix, parts[1], enc.maxOffset)
			reader = io.MultiReader(obj, reader)
			n += latestObj.Size
		}
	}

	if _, err := s.s3.PutObject(ctx, s.cfg.S3.Bucket, key, reader, n, minio.PutObjectOptions{
		ContentType:     "application/gzip",
		ContentEncoding: "gzip",
	}); err != nil {
		return err
	}

	if removeLatest {
		if err := s.s3.RemoveObject(ctx, s.cfg.S3.Bucket, latestObj.Key, minio.RemoveObjectOptions{}); err != nil {
			log.Err(err).Msgf("S3: Remove %q", latestObj.Key)
		}
	}
	return nil
}

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
	return e.gzw.Close()
}

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

//func (s *Sink) listenMerge(ctx context.Context, topic string, wg *sync.WaitGroup) {
//	return
//	defer wg.Done()
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case <-time.After(s.cfg.MergeTick):
//			for {
//				if err := s.merge(topic); err != nil && !errors.Is(err, context.Canceled) {
//					log.Err(err).Msgf("S3: Merge")
//				}
//				select {
//				case <-time.After(5 * time.Second):
//				case <-ctx.Done():
//					return
//				}
//			}
//		}
//	}
//}
//
//func (s *Sink) merge(topic string) error {
//	type group struct {
//		size int64
//		keys []string
//	}
//
//	//language=goregexp
//	regex, err := regexp.Compile(fmt.Sprintf(`(%s/\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d)[/_]\w+\.gz`, topic))
//	if err != nil {
//		return err
//	}
//
//	ctx := context.Background()
//	groups := make(map[string]group)
//	for obj := range s.s3.ListObjects(ctx, s.cfg.S3.Bucket, minio.ListObjectsOptions{
//		Prefix:     topic,
//		Recursive:  true,
//		StartAfter: topic + "/" + time.Now().UTC().Add(-s.cfg.MergeOffset).Format(time.DateTime),
//	}) {
//		if obj.Err != nil {
//			return obj.Err
//		}
//		if obj.Size == 0 {
//			continue
//		}
//		parts := regex.FindStringSubmatch(obj.Key)
//		if len(parts) < 2 {
//			continue
//		}
//		prefix := parts[1]
//		g := groups[prefix]
//		g.keys = append(g.keys, obj.Key)
//		g.size += obj.Size
//		groups[prefix] = g
//	}
//
//	var updPrefixes []string
//	for prefix, group := range groups {
//		if len(group.keys) < 2 {
//			continue
//		}
//
//		readers := make([]io.Reader, len(group.keys))
//		for i, key := range group.keys {
//			obj, err := s.s3.GetObject(ctx, s.cfg.S3.Bucket, key, minio.GetObjectOptions{})
//			if err != nil {
//				return err
//			}
//			readers[i] = obj
//		}
//
//		if _, err := s.s3.PutObject(
//			ctx,
//			s.cfg.S3.Bucket,
//			fmt.Sprintf("%s/%d.gz", prefix, s.snowflake.Generate()),
//			io.MultiReader(readers...),
//			group.size,
//			minio.PutObjectOptions{
//				ContentType:           "application/gzip",
//				ContentEncoding:       "gzip",
//				ConcurrentStreamParts: true,
//				NumThreads:            5,
//			},
//		); err != nil {
//			return err
//		}
//
//		var g errgroup.Group
//		for _, key := range group.keys {
//			g.Go(func() error {
//				return s.s3.RemoveObject(ctx, s.cfg.S3.Bucket, key, minio.RemoveObjectOptions{})
//			})
//		}
//		if err := g.Wait(); err != nil {
//			return err
//		}
//		updPrefixes = append(updPrefixes, prefix)
//	}
//
//	if len(updPrefixes) > 0 {
//		log.Info().Strs("prefixes", updPrefixes).Msg("S3: Keys have been merged")
//	}
//	return nil
//}

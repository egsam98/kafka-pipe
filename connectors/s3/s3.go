package s3

import (
	"regexp"
	"strconv"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

var regexKey = regexp.MustCompile(`^(.+)/(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d)/(\d+)/(\d+)-(\d+).gz$`)

type keySegments []string

func newKeySegments(key string) (keySegments, error) {
	segments := regexKey.FindStringSubmatch(key)
	if len(segments) < 6 {
		return nil, errors.Errorf("S3: invalid key: %s", key)
	}
	return segments, nil
}

func (s keySegments) topic() string { return s[1] }

func (s keySegments) dateTime() (time.Time, error) {
	return time.Parse(time.DateTime, s[2])
}

func (s keySegments) partition() (int32, error) {
	i64, err := strconv.ParseInt(s[3], 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(i64), nil
}

func (s keySegments) minOffset() (int64, error) { return strconv.ParseInt(s[4], 10, 64) }

func (s keySegments) maxOffset() (int64, error) { return strconv.ParseInt(s[5], 10, 64) }

// jsonRow represents a line of encoded data in S3
type jsonRow struct {
	Offset  int64    `json:"offset"`
	Key     string   `json:"key"`
	Value   string   `json:"value"`
	Headers []header `json:"headers"`
}

type header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func newJsonRow(rec *kgo.Record) jsonRow {
	r := jsonRow{
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

func (j *jsonRow) kafkaRecord(topic string, partition int32) kgo.Record {
	rec := kgo.Record{
		Key:       unsafe.Slice(unsafe.StringData(j.Key), len(j.Key)),
		Value:     unsafe.Slice(unsafe.StringData(j.Value), len(j.Value)),
		Topic:     topic,
		Partition: partition,
		Headers:   make([]kgo.RecordHeader, len(j.Headers)),
	}
	for i, h := range j.Headers {
		rec.Headers[i] = kgo.RecordHeader{
			Key:   h.Key,
			Value: unsafe.Slice(unsafe.StringData(h.Value), len(h.Value)),
		}
	}
	return rec
}

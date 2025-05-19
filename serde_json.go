package kafkapipe

import (
	"encoding/base64"
	"io"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

func init() {
	jsoniter.ConfigDefault = jsoniter.Config{
		EscapeHTML:                    true,
		SortMapKeys:                   true,
		ValidateJsonRawMessage:        true,
		MarshalFloatWith6Digits:       true,
		ObjectFieldMustBeSimpleString: true,
	}.Froze()
	jsoniter.RegisterTypeDecoderFunc("time.Time", timeDecoder)
	jsoniter.RegisterTypeDecoderFunc("[]uint8", bytesDecoder)
}

type TimeFormat string

func (t *TimeFormat) UnmarshalText(text []byte) error {
	switch val := TimeFormat(text); val {
	case "":
		*t = TimestampMilli
	case RFC3339, TimestampMilli, Timestamp:
		*t = val
	default:
		return errors.Errorf("invalid time_format: %s", text)
	}
	return nil
}

const (
	RFC3339        TimeFormat = "rfc3339"
	Timestamp      TimeFormat = "timestamp"
	TimestampMilli TimeFormat = "timestamp-milli"
)

type JSON struct {
	timeFormat TimeFormat
}

func NewJSON(timeFormat TimeFormat) *JSON {
	return &JSON{timeFormat: timeFormat}
}

func newJSONFromYAML(value yaml.Node) (*JSON, error) {
	var cfg struct {
		TimeFormat TimeFormat `yaml:"time_format"`
	}
	if err := value.Decode(&cfg); err != nil {
		return nil, errors.Wrapf(err, "decode yaml %q into %T", value.Value, cfg)
	}
	return NewJSON(cfg.TimeFormat), nil
}

func (j *JSON) Deserialize(dst any, _ string, src []byte) error {
	iter := jsoniter.ConfigDefault.BorrowIterator(src)
	defer jsoniter.ConfigDefault.ReturnIterator(iter)
	iter.Attachment = j
	iter.ReadVal(dst)
	if iter.Error == io.EOF {
		return nil
	}
	return iter.Error
}

func (*JSON) Tag() string { return "json" }

func timeDecoder(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	format := RFC3339
	if j, ok := iter.Attachment.(*JSON); ok {
		format = j.timeFormat
	}

	null := iter.ReadNil()
	if iter.Error != nil {
		return
	}
	if null {
		*(*time.Time)(ptr) = time.Time{}
		return
	}

	var t time.Time
	switch format {
	case RFC3339:
		str := iter.ReadString()
		if iter.Error != nil {
			return
		}
		t, iter.Error = time.Parse(time.RFC3339, str)
	case Timestamp:
		t = time.Unix(iter.ReadInt64(), 0)
	case TimestampMilli:
		t = time.UnixMilli(iter.ReadInt64())
	}

	if iter.Error != nil {
		return
	}
	*((*time.Time)(ptr)) = t
}

func bytesDecoder(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	var bytes []byte

	switch iter.WhatIsNext() {
	case jsoniter.NilValue:
		iter.Skip()
	case jsoniter.StringValue:
		src := iter.ReadString()
		if iter.Error != nil {
			return
		}
		bytes, iter.Error = base64.StdEncoding.DecodeString(src)
	default:
		bytes = iter.SkipAndReturnBytes()
	}

	if iter.Error != nil {
		return
	}
	*(*[]byte)(ptr) = bytes
}

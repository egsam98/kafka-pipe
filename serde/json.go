package serde

import (
	"io"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
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
}

type TimeFormat string

const (
	RFC3339        TimeFormat = "RFC3339"
	Timestamp      TimeFormat = "timestamp"
	TimestampMilli TimeFormat = "timestamp-milli"
)

type JSON struct {
	timeFormat TimeFormat
}

func NewJSON(timeFormat TimeFormat) *JSON {
	return &JSON{timeFormat: timeFormat}
}

func (j *JSON) Deserialize(dst any, src []byte) error {
	iter := jsoniter.ConfigDefault.BorrowIterator(src)
	defer jsoniter.ConfigDefault.ReturnIterator(iter)
	iter.Attachment = j
	iter.ReadVal(dst)
	if iter.Error == io.EOF {
		return nil
	}
	return iter.Error
}

func timeDecoder(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	format := RFC3339
	if j, ok := iter.Attachment.(*JSON); ok {
		format = j.timeFormat
	}

	var t time.Time
	switch format {
	case RFC3339:
		var err error
		if t, err = time.Parse(time.RFC3339, iter.ReadString()); err != nil {
			iter.Error = err
			return
		}
	case Timestamp:
		t = time.Unix(iter.ReadInt64(), 0)
	case TimestampMilli:
		t = time.UnixMilli(iter.ReadInt64())
	}

	*((*time.Time)(ptr)) = t
}
package pg

import (
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pkg/errors"
)

const TimestampFormat = "2006-01-02 15:04:05.999999999"

type UUIDCodec struct {
	pgtype.UUIDCodec
}

func (c UUIDCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	switch format {
	case pgtype.TextFormatCode:
		return string(src), nil
	case pgtype.BinaryFormatCode:
		res, err := c.UUIDCodec.DecodeValue(m, oid, format, src)
		if err != nil {
			return nil, err
		}
		val := res.([16]byte)
		return fmt.Sprintf("%x-%x-%x-%x-%x", val[0:4], val[4:6], val[6:8], val[8:10], val[10:16]), nil
	default:
		return nil, errors.Errorf("unknown format code: %d", format)
	}
}

type NumericCodec struct {
	pgtype.NumericCodec
}

func (c NumericCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	switch format {
	case pgtype.TextFormatCode:
		return string(src), nil
	case pgtype.BinaryFormatCode:
		res, err := c.NumericCodec.DecodeValue(m, oid, format, src)
		if err != nil {
			return nil, err
		}
		return res.(pgtype.Numeric).Value()
	default:
		return nil, errors.Errorf("unknown format code: %d", format)
	}
}

type TimestampCodec struct {
	pgtype.TimestampCodec
}

func (c TimestampCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	switch format {
	case pgtype.TextFormatCode:
		t, err := time.Parse(TimestampFormat, string(src))
		if err != nil {
			return nil, err
		}
		return t.UnixMicro(), nil
	case pgtype.BinaryFormatCode:
		res, err := c.TimestampCodec.DecodeValue(m, oid, format, src)
		if err != nil {
			return nil, err
		}
		switch res := res.(type) {
		case time.Time:
			return res.UnixMicro(), nil
		case pgtype.InfinityModifier:
			if res == pgtype.Infinity {
				return math.MaxInt64, nil
			}
			return math.MinInt64, nil
		default:
			return nil, errors.Errorf("unexpected TimestampCodec.DecodeValue result type: %T", res)
		}
	default:
		return nil, errors.Errorf("unknown format code: %d", format)
	}
}

package pg

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pkg/errors"
)

type UUIDCodec struct {
	pgtype.UUIDCodec
}

func (c UUIDCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
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

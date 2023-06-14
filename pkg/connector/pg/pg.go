package pg

import (
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pkg/errors"
)

type ErrorCode string

const (
	DuplicateObject ErrorCode = "42710"
)

func Is(err error, code ErrorCode) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == string(code)
}

func RegisterTypes(typeMap *pgtype.Map) {
	typeMap.RegisterType(&pgtype.Type{
		Codec: UUIDCodec{},
		Name:  "uuid",
		OID:   pgtype.UUIDOID,
	})
	typeMap.RegisterType(&pgtype.Type{
		Codec: NumericCodec{},
		Name:  "numeric",
		OID:   pgtype.NumericOID,
	})
	typeMap.RegisterType(&pgtype.Type{
		Codec: TimestampCodec{},
		Name:  "timestamp",
		OID:   pgtype.TimestampOID,
	})
}

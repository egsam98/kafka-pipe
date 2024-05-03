package pg

import (
	"fmt"

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

func registerTypes(typeMap *pgtype.Map) {
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

func kafkaKey(data map[string]any) ([]byte, error) {
	dataID, ok := data["id"]
	if !ok {
		return nil, errors.Errorf(`default ID key "id" is not specified for data: %v`, data)
	}
	var key string
	if _, ok := dataID.(string); ok {
		key = fmt.Sprintf(`{"id": %q}`, dataID)
	} else {
		key = fmt.Sprintf(`{"id": %v}`, dataID)
	}
	return []byte(key), nil
}

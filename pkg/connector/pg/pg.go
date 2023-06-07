package pg

import (
	"github.com/jackc/pgx/v5/pgconn"
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

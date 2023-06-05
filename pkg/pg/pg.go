package pg

import (
	"strconv"

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

func NumericToString(n pgtype.Numeric) string {
	buf := make([]byte, 0, 76)
	if n.Int == nil {
		buf = append(buf, '0')
	} else {
		buf = append(buf, n.Int.String()...)
	}
	buf = append(buf, 'e')
	buf = append(buf, strconv.FormatInt(int64(n.Exp), 10)...)
	return string(buf)
}

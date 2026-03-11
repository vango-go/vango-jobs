package jobs

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is the PostgreSQL contract required by the durable runtime.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
	Ping(ctx context.Context) error
}

type connAcquirer interface {
	Acquire(context.Context) (*pgxpool.Conn, error)
}

// Open creates a PostgreSQL-backed jobs runtime.
func Open(db DB, reg *Registry, opts ...RuntimeOption) (*Runtime, error) {
	if db == nil {
		return nil, ErrInvalidDefinition
	}
	return NewRuntime(newPostgresStore(db, opts...), reg, opts...)
}

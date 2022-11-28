package pgxpool

import (
	"context"
	"errors"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type Tx struct {
	t pgx.Tx
	c *Conn
}

func (tx *Tx) Begin(ctx context.Context) (pgx.Tx, error) {
	return tx.t.Begin(ctx)
}

func (tx *Tx) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	return tx.t.BeginFunc(ctx, f)
}

var ctxAlreadyDone *pgconn.СontextAlreadyDoneError

func (tx *Tx) Commit(ctx context.Context) error {
	err := tx.t.Commit(ctx)
	if tx.c != nil {
		// Force connection cleanup if returned error is a context cancellation.
		// In this case the connection is recovered with rollback and then
		// returned to the pool. In other cases connection gets destroyed
		// in the background if there is an error (inside a Release() method).
		if err != nil && errors.Is(err, ctx.Err()) {
			conn := tx.c.Conn().PgConn()
			// We don't want to clean the socket up if we didn't write anything
			// in rollback yet and connection is not required to be cleaned up.
			// Otherwise, we will hang on reading from socket that has
			// no data being sent back to the server.
			if errors.As(err, &ctxAlreadyDone) && !conn.NeedsCleanup() {
				conn.SetCleanupWithoutReset(true)
			}

			conn.SetNeedsCleanup()
		}

		tx.c.Release()
		tx.c = nil
	}

	// Do not show a cleanup errors to the user.
	if errors.Is(err, pgconn.ErrLockCleanupConn) {
		return nil
	}

	return err
}

func (tx *Tx) Rollback(ctx context.Context) error {
	err := tx.t.Rollback(ctx)
	if tx.c != nil {
		// Force connection cleanup if returned error is a context cancellation.
		// In this case the connection is recovered with rollback and then
		// returned to the pool. In other cases connection gets destroyed
		// in the background if there is an error (inside a Release() method).
		if err != nil && errors.Is(err, ctx.Err()) {
			conn := tx.c.Conn().PgConn()

			// We don't want to clean the socket up if we didn't write anything
			// in rollback yet and connection is not required to be cleaned up.
			// Otherwise, we will hang on reading from socket that has
			// no data being sent back to the server.
			if errors.As(err, &ctxAlreadyDone) && !conn.NeedsCleanup() {
				conn.SetCleanupWithoutReset(true)
			}

			conn.SetNeedsCleanup()
		}

		tx.c.Release()
		tx.c = nil
	}

	// Do not show a cleanup errors to the user.
	if errors.Is(err, pgconn.ErrLockCleanupConn) {
		return nil
	}

	return err
}

func (tx *Tx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return tx.t.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (tx *Tx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return tx.t.SendBatch(ctx, b)
}

func (tx *Tx) LargeObjects() pgx.LargeObjects {
	return tx.t.LargeObjects()
}

func (tx *Tx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return tx.t.Prepare(ctx, name, sql)
}

func (tx *Tx) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return tx.t.Exec(ctx, sql, arguments...)
}

func (tx *Tx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return tx.t.Query(ctx, sql, args...)
}

func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return tx.t.QueryRow(ctx, sql, args...)
}

func (tx *Tx) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	return tx.t.QueryFunc(ctx, sql, args, scans, f)
}

func (tx *Tx) Conn() *pgx.Conn {
	return tx.t.Conn()
}

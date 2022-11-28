package pgxpool

import (
	"context"
	"errors"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type RetryPool struct {
	p *Pool
}

func (p *RetryPool) Close() {
	p.p.Close()
}

func (p *RetryPool) Acquire(ctx context.Context) (*Conn, error) {
	return p.p.Acquire(ctx)
}

func (p *RetryPool) AcquireFunc(ctx context.Context, f func(*Conn) error) error {
	return p.p.AcquireFunc(ctx, f)
}

func (p *RetryPool) AcquireAllIdle(ctx context.Context) []*Conn {
	return p.p.AcquireAllIdle(ctx)
}

func (p *RetryPool) Config() *Config { return p.p.Config() }

func (p *RetryPool) Stat() *Stat {
	return p.p.Stat()
}

func (p *RetryPool) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	var tag pgconn.CommandTag
	var err error
	p.withRetry(func() error {
		tag, err = p.p.Exec(ctx, sql, arguments...)
		return err
	})

	return tag, err
}

func (p *RetryPool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	var rows pgx.Rows
	var err error
	p.withRetry(func() error {
		rows, err = p.p.Query(ctx, sql, args...)
		return err
	})

	return rows, err
}

func (p *RetryPool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return p.p.QueryRow(ctx, sql, args...)
}

func (p *RetryPool) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	var tag pgconn.CommandTag
	var err error
	p.withRetry(func() error {
		tag, err = p.p.QueryFunc(ctx, sql, args, scans, f)
		return err
	})

	return tag, err
}

func (p *RetryPool) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return p.p.SendBatch(ctx, b)
}

func (p *RetryPool) Begin(ctx context.Context) (pgx.Tx, error) {
	var tx pgx.Tx
	var err error
	p.withRetry(func() error {
		tx, err = p.p.Begin(ctx)
		return err
	})

	return tx, err
}

func (p *RetryPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	var tx pgx.Tx
	var err error
	p.withRetry(func() error {
		tx, err = p.p.BeginTx(ctx, txOptions)
		return err
	})

	return tx, err
}

func (p *RetryPool) BeginFunc(ctx context.Context, f func(pgx.Tx) error) error {
	var err error
	p.withRetry(func() error {
		err = p.p.BeginFunc(ctx, f)
		return err
	})

	return err
}

func (p *RetryPool) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error {
	var err error
	p.withRetry(func() error {
		err = p.p.BeginTxFunc(ctx, txOptions, f)
		return err
	})

	return err
}

func (p *RetryPool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	var r int64
	var err error
	p.withRetry(func() error {
		r, err = p.p.CopyFrom(ctx, tableName, columnNames, rowSrc)
		return err
	})

	return r, err
}

func (p *RetryPool) Ping(ctx context.Context) error {
	var err error
	p.withRetry(func() error {
		err = p.p.Ping(ctx)
		return err
	})

	return err
}

func (p *RetryPool) safeToRetry(err error) bool {
	for err != nil {
		if pgconn.SafeToRetry(err) {
			return true
		}

		err = errors.Unwrap(err)
	}

	return false
}

func (p *RetryPool) withRetry(f func() error) {
	for i := p.p.config.MaxConns; i > 0; i-- {
		err := f()
		if err == nil || !p.safeToRetry(err) {
			return
		}
	}
}

func wrapInRetryPool(pool *Pool) *RetryPool {
	return &RetryPool{p: pool}
}

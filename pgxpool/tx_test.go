package pgxpool_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestTxExec(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testExec(t, tx)
}

func TestTxQuery(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testQuery(t, tx)
}

func TestTxQueryRow(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testQueryRow(t, tx)
}

func TestTxSendBatch(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testSendBatch(t, tx)
}

func TestTxCopyFrom(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testCopyFrom(t, tx)
}

func TestTxRecover(t *testing.T) {
	pool := newSimpleProtoPool(t)
	_, err := pool.Exec(context.Background(), `create table if not exists test ( val int );`)
	require.NoError(t, err)
	t.Cleanup(func() {
		defer pool.Close()

		_, err = pool.Exec(context.Background(), `drop table test`)
		require.NoError(t, err)
	})

	runWithSimpleProtoPool := func(t *testing.T, f func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool)) {
		t.Run("SimpleProto", func(t *testing.T) {
			p := newSimpleProtoPool(t)
			defer p.Close()

			tx := testNewTx(t, p)
			defer tx.Rollback(context.Background())

			f(t, tx, p)
		})
	}

	runWithExtendedProto := func(t *testing.T, f func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool)) {
		t.Run("ExtendedProto", func(t *testing.T) {
			p := newExtendedQueryProtoPool(t)
			defer p.Close()

			tx := testNewTx(t, p)
			defer tx.Rollback(context.Background())

			f(t, tx, p)
		})
	}

	run := func(t *testing.T, f func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool)) {
		runWithSimpleProtoPool(t, f)
		runWithExtendedProto(t, f)
	}

	t.Run("ExecRollbackCtxCanceled", func(t *testing.T) {
		f := func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)

			_, err := tx.Exec(ctx, `insert into test values ($1);`, 15)
			require.NoError(t, err)

			cancel()
			err = tx.Rollback(ctx)
			require.Error(t, err)

			time.Sleep(time.Millisecond * 100)
			rollbackChecks(t, p, `insert into test values (15)`)

			assertConnectionHasNoUnwantedData(t, p)
		}

		run(t, f)
	})

	t.Run("ExecCommitCtxCanceled", func(t *testing.T) {
		f := func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)

			_, err := tx.Exec(ctx, `insert into test values ($1);`, 14)
			require.NoError(t, err)

			cancel()
			err = tx.Commit(ctx)
			require.Error(t, err)

			time.Sleep(time.Millisecond * 100)
			rollbackChecks(t, p, `insert into test values (14)`)

			assertConnectionHasNoUnwantedData(t, p)
		}

		run(t, f)
	})

	t.Run("ExecCtxAllCanceledCommit", func(t *testing.T) {

		f := func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := tx.Exec(ctx, `insert into test values ($1);`, 13)
			require.Error(t, err)

			err = tx.Commit(ctx)
			require.Error(t, err)

			time.Sleep(time.Millisecond * 100)
			rollbackChecks(t, p, `insert into test values (13)`)

			// check that there is no garbage in connection
			assertConnectionHasNoUnwantedData(t, p)
		}

		run(t, f)
	})

	t.Run("ExecCtxAllCanceledRollback", func(t *testing.T) {

		f := func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := tx.Exec(ctx, `insert into test values ($1);`, 12)
			require.Error(t, err)

			err = tx.Rollback(ctx)
			require.Error(t, err)

			time.Sleep(time.Millisecond * 100)
			rollbackChecks(t, p, `insert into test values (12)`)

			// check that there is no garbage in connection
			assertConnectionHasNoUnwantedData(t, p)
		}

		run(t, f)
	})

	t.Run("ExecCtxExecCanceledRollback", func(t *testing.T) {

		f := func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()

			_, err := tx.Exec(ctx, `insert into test values ($1)`, 11)
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `select pg_sleep($1)`, 0.07)
			require.Error(t, err)

			err = tx.Rollback(context.Background())
			require.NoError(t, err)

			time.Sleep(time.Millisecond * 300)
			rollbackChecks(t, p, `select pg_sleep(0.07)`)

			assertConnectionHasNoUnwantedData(t, p)
		}

		run(t, f)
	})

	t.Run("ExecCtxExecCanceledCommit", func(t *testing.T) {

		f := func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()

			_, err := tx.Exec(ctx, `insert into test values ($1);`, 24)
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `select pg_sleep($1)`, 0.08)
			require.Error(t, err)

			err = tx.Commit(context.Background())
			require.NoError(t, err)

			time.Sleep(time.Millisecond * 300)
			rollbackChecks(t, p, `select pg_sleep(0.08)`)

			assertConnectionHasNoUnwantedData(t, p)
		}

		run(t, f)
	})

	t.Run("Commit", func(t *testing.T) {

		f := func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool) {
			ctx := context.Background()
			_, err := tx.Exec(ctx, `create table if not exists temp_test ( val int );`)
			require.NoError(t, err)

			t.Cleanup(func() {
				cleanPool := newExtendedQueryProtoPool(t)
				defer cleanPool.Close()

				_, err := cleanPool.Exec(ctx, `drop table temp_test;`)
				require.NoError(t, err)
			})

			_, err = tx.Exec(ctx, `insert into temp_test values ($1)`, 10)
			require.NoError(t, err)

			err = tx.Commit(ctx)
			require.NoError(t, err)

			time.Sleep(time.Millisecond * 100)
			commitChecks(t, p, `insert into temp_test values (10)`)
		}

		run(t, f)
	})

	t.Run("WrongQuery", func(t *testing.T) {
		f := func(t *testing.T, tx pgx.Tx, p *pgxpool.RetryPool) {
			ctx := context.Background()
			_, err = tx.Exec(ctx, `insert into blablabla values ($1)`, 10)
			require.Error(t, err)

			err = tx.Commit(ctx)
			require.ErrorIs(t, err, pgx.ErrTxCommitRollback)

			rollbackChecks(t, p, `insert into blablabla values (10)`)
		}

		run(t, f)
	})
}

func commitChecks(t *testing.T, pool *pgxpool.RetryPool, sql string) {
	t.Helper()
	require.Equal(t, int32(1), pool.Stat().IdleConns(), "pool idle connections")
	require.Equal(t, int32(1), pool.Stat().TotalConns(), "pool total connections")
	checkForRecords(t, pool, "temp_test", 1)
	assertNoIdleTransactions(t, pool, sql)
}

func rollbackChecks(t *testing.T, pool *pgxpool.RetryPool, sql string) {
	t.Helper()
	require.Equal(t, int32(1), pool.Stat().IdleConns(), "pool idle connections")
	require.Equal(t, int32(1), pool.Stat().TotalConns(), "pool total connections")
	checkForRecords(t, pool, "test", 0)
	assertNoIdleTransactions(t, pool, sql)
}

func checkForRecords(t *testing.T, pool *pgxpool.RetryPool, tableName string, recordNum int) {
	t.Helper()
	r, err := pool.Query(context.Background(), `select * from `+tableName)
	require.NoError(t, err)
	counter := 0
	for r.Next() {
		counter++
	}
	require.Equal(t, recordNum, counter, "record number")
}

func assertNoIdleTransactions(t *testing.T, pool *pgxpool.RetryPool, sql string) {
	t.Helper()
	r, err := pool.Query(context.Background(), `select query from pg_stat_activity
		where (state = 'idle in transaction') and xact_start is not null;`)
	require.NoError(t, err)
	for r.Next() {
		var query string
		err = r.Scan(&query)
		require.NoError(t, err)
		require.NotEqual(t, sql, query, "idle transaction")
	}
}

func newSimpleProtoPool(t *testing.T, opts ...func(config *pgxpool.Config)) *pgxpool.RetryPool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	cfg.ConnConfig.PreferSimpleProtocol = true
	cfg.LazyConnect = true

	for _, opt := range opts {
		opt(cfg)
	}

	pool, err := pgxpool.ConnectConfig(context.Background(), cfg)
	require.NoError(t, err)

	return pool
}

func newExtendedQueryProtoPool(t *testing.T, opts ...func(config *pgxpool.Config)) *pgxpool.RetryPool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	cfg.LazyConnect = true

	for _, opt := range opts {
		opt(cfg)
	}

	pool, err := pgxpool.ConnectConfig(context.Background(), cfg)
	require.NoError(t, err)

	return pool
}

func testNewTx(t *testing.T, pool *pgxpool.RetryPool) pgx.Tx {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	return tx
}

func assertConnectionHasNoUnwantedData(t *testing.T, pool *pgxpool.RetryPool) {
	t.Helper()
	rr, err := pool.Query(context.Background(), `select * from generate_series(0, 9)`)
	require.NoError(t, err)
	counter := 0
	for rr.Next() {
		var i int
		err = rr.Scan(&i)
		require.NoError(t, err)
		require.Equal(t, counter, i)
		counter++
	}
	require.Equal(t, 10, counter)
}

package pgxpool

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newRetryPool(t *testing.T) *RetryPool {
	t.Helper()

	cfg, err := ParseConfig(os.Getenv("PGX_TEST_DATABASE_PROXY"))
	require.NoError(t, err)
	cfg.ConnConfig.PreferSimpleProtocol = true
	cfg.LazyConnect = true
	cfg.ConnCleanupTimeout = time.Hour

	p, err := ConnectConfig(context.Background(), cfg)
	require.NoError(t, err)

	return p
}

type mockSafeToRetryErr struct{}

func (*mockSafeToRetryErr) Error() string     { return "fake err" }
func (*mockSafeToRetryErr) SafeToRetry() bool { return true }

func TestRetryFailure(t *testing.T) {
	p := newRetryPool(t)
	p.p.config.MaxConns = 3

	errFake := &mockSafeToRetryErr{}
	var err error
	var counter int32
	p.withRetry(func() error {
		counter++
		err = errFake
		return err
	})

	// check for return of last error
	require.Equal(t, err, errFake)
	// check for max attempts to retry
	require.Equal(t, p.p.config.MaxConns, counter)
}

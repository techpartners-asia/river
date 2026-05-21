package riverpilot

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// periodicJobExecutorStub captures invocations to the periodic-job executor
// methods so tests can assert what the pilot delegated. Other Executor
// methods are unimplemented; calling them in a test will panic on the
// embedded nil interface, which is the intent.
type periodicJobExecutorStub struct {
	riverdriver.Executor

	getCalled         int
	keepAliveCalled   int
	upsertCalled      int
	lastKeepAlive     *riverdriver.PeriodicJobKeepAliveAndReapParams
	lastUpsert        *riverdriver.PeriodicJobUpsertManyParams
	lastGet           *riverdriver.PeriodicJobGetAllParams
	getReturn         []*rivertype.DurablePeriodicJob
	keepAliveReturn   []*rivertype.DurablePeriodicJob
	upsertReturn      []*rivertype.DurablePeriodicJob
	keepAliveReturnEr error
	upsertReturnErr   error
	getReturnErr      error
}

func (e *periodicJobExecutorStub) PeriodicJobGetAll(ctx context.Context, params *riverdriver.PeriodicJobGetAllParams) ([]*rivertype.DurablePeriodicJob, error) {
	e.getCalled++
	e.lastGet = params
	return e.getReturn, e.getReturnErr
}

func (e *periodicJobExecutorStub) PeriodicJobKeepAliveAndReap(ctx context.Context, params *riverdriver.PeriodicJobKeepAliveAndReapParams) ([]*rivertype.DurablePeriodicJob, error) {
	e.keepAliveCalled++
	e.lastKeepAlive = params
	return e.keepAliveReturn, e.keepAliveReturnEr
}

func (e *periodicJobExecutorStub) PeriodicJobUpsertMany(ctx context.Context, params *riverdriver.PeriodicJobUpsertManyParams) ([]*rivertype.DurablePeriodicJob, error) {
	e.upsertCalled++
	e.lastUpsert = params
	return e.upsertReturn, e.upsertReturnErr
}

func TestStandardPilot_DurableDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pilot := &StandardPilot{}
	stub := &periodicJobExecutorStub{}

	got, err := pilot.PeriodicJobGetAll(ctx, stub, &PeriodicJobGetAllParams{Schema: "x"})
	require.NoError(t, err)
	require.Nil(t, got)
	require.Zero(t, stub.getCalled, "executor must not be called when disabled")

	upserted, err := pilot.PeriodicJobUpsertMany(ctx, stub, &PeriodicJobUpsertManyParams{
		Jobs: []*PeriodicJobUpsertParams{{ID: "x", NextRunAt: time.Now(), UpdatedAt: time.Now()}},
	})
	require.NoError(t, err)
	require.Nil(t, upserted)
	require.Zero(t, stub.upsertCalled, "executor must not be called when disabled")

	reaped, err := pilot.PeriodicJobKeepAliveAndReap(ctx, stub, &PeriodicJobKeepAliveAndReapParams{ID: []string{"x"}})
	require.NoError(t, err)
	require.Nil(t, reaped)
	require.Zero(t, stub.keepAliveCalled, "executor must not be called when disabled")
}

func TestStandardPilot_DurableEnabled_Delegates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pilot := &StandardPilot{
		DurablePeriodicJobs: StandardPilotDurablePeriodicJobsConfig{
			Enabled:        true,
			StaleThreshold: time.Hour,
		},
	}

	stub := &periodicJobExecutorStub{
		getReturn: []*rivertype.DurablePeriodicJob{
			{ID: "a", NextRunAt: time.Now()},
		},
		upsertReturn: []*rivertype.DurablePeriodicJob{
			{ID: "a", NextRunAt: time.Now()},
		},
		keepAliveReturn: []*rivertype.DurablePeriodicJob{
			{ID: "stale", NextRunAt: time.Now()},
		},
	}

	got, err := pilot.PeriodicJobGetAll(ctx, stub, &PeriodicJobGetAllParams{Schema: "s1"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "a", got[0].ID)
	require.Equal(t, 1, stub.getCalled)
	require.Equal(t, "s1", stub.lastGet.Schema)

	upserted, err := pilot.PeriodicJobUpsertMany(ctx, stub, &PeriodicJobUpsertManyParams{
		Schema: "s1",
		Jobs: []*PeriodicJobUpsertParams{
			{ID: "a", NextRunAt: time.Now(), UpdatedAt: time.Now()},
			{ID: "", NextRunAt: time.Now(), UpdatedAt: time.Now()}, // filtered out
		},
	})
	require.NoError(t, err)
	require.Len(t, upserted, 1)
	require.Equal(t, 1, stub.upsertCalled)
	require.Len(t, stub.lastUpsert.Jobs, 1, "IDless jobs must be filtered out before delegation")

	now := time.Now()
	reaped, err := pilot.PeriodicJobKeepAliveAndReap(ctx, stub, &PeriodicJobKeepAliveAndReapParams{
		Schema: "s1",
		ID:     []string{"a"},
	})
	require.NoError(t, err)
	require.Len(t, reaped, 1)
	require.Equal(t, 1, stub.keepAliveCalled)
	require.Equal(t, []string{"a"}, stub.lastKeepAlive.ID)
	require.WithinDuration(t, now.Add(-time.Hour), stub.lastKeepAlive.StaleHorizon, 2*time.Second,
		"StaleHorizon must be now - threshold")
	require.NotNil(t, stub.lastKeepAlive.Now, "Now must be passed through to executor")
}

func TestStandardPilot_PeriodicJobUpsertMany_EmptyJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pilot := &StandardPilot{
		DurablePeriodicJobs: StandardPilotDurablePeriodicJobsConfig{Enabled: true},
	}
	stub := &periodicJobExecutorStub{}

	res, err := pilot.PeriodicJobUpsertMany(ctx, stub, &PeriodicJobUpsertManyParams{})
	require.NoError(t, err)
	require.Nil(t, res)
	require.Zero(t, stub.upsertCalled)
}

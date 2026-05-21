package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
)

func exercisePeriodicJob[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()
		exec, driver := executorWithTx(ctx, t)
		return exec, &testBundle{driver: driver}
	}

	t.Run("PeriodicJob", func(t *testing.T) {
		t.Parallel()

		t.Run("UpsertManyInsertsNewRows", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Truncate(time.Second)
			nextA := now.Add(15 * time.Minute)
			nextB := now.Add(30 * time.Minute)

			res, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
				Jobs: []riverdriver.PeriodicJobUpsertManyParamsJob{
					{ID: "job_a", NextRunAt: nextA, UpdatedAt: now},
					{ID: "job_b", NextRunAt: nextB, UpdatedAt: now},
				},
			})
			require.NoError(t, err)
			require.Len(t, res, 2)

			byID := make(map[string]struct {
				NextRunAt time.Time
				CreatedAt time.Time
				UpdatedAt time.Time
			})
			for _, r := range res {
				byID[r.ID] = struct {
					NextRunAt time.Time
					CreatedAt time.Time
					UpdatedAt time.Time
				}{r.NextRunAt, r.CreatedAt, r.UpdatedAt}
			}
			require.WithinDuration(t, nextA, byID["job_a"].NextRunAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, nextB, byID["job_b"].NextRunAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now, byID["job_a"].UpdatedAt, bundle.driver.TimePrecision())
		})

		t.Run("UpsertManyUpdatesExistingRows", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Truncate(time.Second)
			firstNext := now.Add(15 * time.Minute)
			_, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
				Jobs: []riverdriver.PeriodicJobUpsertManyParamsJob{
					{ID: "job_update", NextRunAt: firstNext, UpdatedAt: now},
				},
			})
			require.NoError(t, err)

			initial, err := exec.PeriodicJobGetAll(ctx, &riverdriver.PeriodicJobGetAllParams{})
			require.NoError(t, err)
			require.Len(t, initial, 1)
			createdAt := initial[0].CreatedAt

			secondNext := now.Add(45 * time.Minute)
			secondUpdated := now.Add(2 * time.Second)
			res, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
				Jobs: []riverdriver.PeriodicJobUpsertManyParamsJob{
					{ID: "job_update", NextRunAt: secondNext, UpdatedAt: secondUpdated},
				},
			})
			require.NoError(t, err)
			require.Len(t, res, 1)
			require.Equal(t, "job_update", res[0].ID)
			require.WithinDuration(t, secondNext, res[0].NextRunAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, secondUpdated, res[0].UpdatedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, createdAt, res[0].CreatedAt, bundle.driver.TimePrecision(),
				"created_at must not change on update")
		})

		t.Run("UpsertManyEmptyJobsReturnsEmpty", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			res, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{})
			require.NoError(t, err)
			require.Empty(t, res)
		})

		t.Run("GetAllReturnsRowsOrderedByID", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC().Truncate(time.Second)
			_, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
				Jobs: []riverdriver.PeriodicJobUpsertManyParamsJob{
					{ID: "c_job", NextRunAt: now.Add(5 * time.Minute), UpdatedAt: now},
					{ID: "a_job", NextRunAt: now.Add(10 * time.Minute), UpdatedAt: now},
					{ID: "b_job", NextRunAt: now.Add(15 * time.Minute), UpdatedAt: now},
				},
			})
			require.NoError(t, err)

			all, err := exec.PeriodicJobGetAll(ctx, &riverdriver.PeriodicJobGetAllParams{})
			require.NoError(t, err)
			require.Len(t, all, 3)
			require.Equal(t, "a_job", all[0].ID)
			require.Equal(t, "b_job", all[1].ID)
			require.Equal(t, "c_job", all[2].ID)
		})

		t.Run("KeepAliveAndReapBumpsUpdatedAtAndReapsOrphans", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Truncate(time.Second)
			oldUpdated := now.Add(-48 * time.Hour)
			recent := now.Add(-5 * time.Minute)
			_, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
				Jobs: []riverdriver.PeriodicJobUpsertManyParamsJob{
					{ID: "registered", NextRunAt: now.Add(time.Hour), UpdatedAt: oldUpdated},
					{ID: "stale_orphan", NextRunAt: now.Add(time.Hour), UpdatedAt: oldUpdated},
					{ID: "fresh_orphan", NextRunAt: now.Add(time.Hour), UpdatedAt: recent},
				},
			})
			require.NoError(t, err)

			staleHorizon := now.Add(-24 * time.Hour)
			reaped, err := exec.PeriodicJobKeepAliveAndReap(ctx, &riverdriver.PeriodicJobKeepAliveAndReapParams{
				ID:           []string{"registered"},
				StaleHorizon: staleHorizon,
				Now:          ptrutil.Ptr(now),
			})
			require.NoError(t, err)
			require.Len(t, reaped, 1)
			require.Equal(t, "stale_orphan", reaped[0].ID)

			all, err := exec.PeriodicJobGetAll(ctx, &riverdriver.PeriodicJobGetAllParams{})
			require.NoError(t, err)
			require.Len(t, all, 2)

			ids := []string{all[0].ID, all[1].ID}
			require.Contains(t, ids, "registered")
			require.Contains(t, ids, "fresh_orphan")

			for _, row := range all {
				if row.ID == "registered" {
					require.WithinDuration(t, now, row.UpdatedAt, bundle.driver.TimePrecision(),
						"registered row's updated_at must be bumped")
				}
			}
		})

		t.Run("KeepAliveAndReapEmptyIDsReapsAllStale", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC().Truncate(time.Second)
			oldUpdated := now.Add(-48 * time.Hour)
			_, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
				Jobs: []riverdriver.PeriodicJobUpsertManyParamsJob{
					{ID: "lone_orphan", NextRunAt: now.Add(time.Hour), UpdatedAt: oldUpdated},
				},
			})
			require.NoError(t, err)

			reaped, err := exec.PeriodicJobKeepAliveAndReap(ctx, &riverdriver.PeriodicJobKeepAliveAndReapParams{
				ID:           nil,
				StaleHorizon: now.Add(-24 * time.Hour),
				Now:          ptrutil.Ptr(now),
			})
			require.NoError(t, err)
			require.Len(t, reaped, 1)
			require.Equal(t, "lone_orphan", reaped[0].ID)

			all, err := exec.PeriodicJobGetAll(ctx, &riverdriver.PeriodicJobGetAllParams{})
			require.NoError(t, err)
			require.Empty(t, all)
		})
	})
}

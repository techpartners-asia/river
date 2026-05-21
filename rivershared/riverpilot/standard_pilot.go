package riverpilot

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

// StandardPilotDurablePeriodicJobsConfig configures the StandardPilot's
// durable periodic job behavior. The river package translates its own
// DurablePeriodicJobsConfig into this struct.
type StandardPilotDurablePeriodicJobsConfig struct {
	Enabled        bool
	StaleThreshold time.Duration
}

type StandardPilot struct {
	seq atomic.Int64

	// DurablePeriodicJobs, when Enabled is true, makes the standard pilot
	// persist periodic job next run times via the configured driver. Should
	// be set before the client calls PilotInit.
	DurablePeriodicJobs StandardPilotDurablePeriodicJobsConfig

	timeGen baseservice.TimeGeneratorWithStub
}

func (p *StandardPilot) JobCleanerQueuesExcluded() []string { return nil }

func (p *StandardPilot) JobGetAvailable(ctx context.Context, exec riverdriver.Executor, state ProducerState, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	if params.MaxToLock <= 0 {
		return nil, nil
	}
	return exec.JobGetAvailable(ctx, params)
}

func (p *StandardPilot) JobCancel(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	return exec.JobCancel(ctx, params)
}

func (p *StandardPilot) JobInsertMany(
	ctx context.Context,
	exec riverdriver.Executor,
	params *riverdriver.JobInsertFastManyParams,
) ([]*riverdriver.JobInsertFastResult, error) {
	return exec.JobInsertFastMany(ctx, params)
}

func (p *StandardPilot) JobRetry(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	return exec.JobRetry(ctx, params)
}

func (p *StandardPilot) JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	return exec.JobSetStateIfRunningMany(ctx, params)
}

func (p *StandardPilot) PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobKeepAliveAndReapParams) ([]*PeriodicJob, error) {
	if !p.DurablePeriodicJobs.Enabled {
		return nil, nil
	}

	staleThreshold := p.DurablePeriodicJobs.StaleThreshold
	if staleThreshold <= 0 {
		staleThreshold = 24 * time.Hour
	}

	now := p.now().UTC()
	reaped, err := exec.PeriodicJobKeepAliveAndReap(ctx, &riverdriver.PeriodicJobKeepAliveAndReapParams{
		ID:           params.ID,
		Schema:       params.Schema,
		StaleHorizon: now.Add(-staleThreshold),
		Now:          &now,
	})
	if err != nil {
		return nil, err
	}
	return periodicJobsFromRiverType(reaped), nil
}

func (p *StandardPilot) PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobGetAllParams) ([]*PeriodicJob, error) {
	if !p.DurablePeriodicJobs.Enabled {
		return nil, nil
	}

	rows, err := exec.PeriodicJobGetAll(ctx, &riverdriver.PeriodicJobGetAllParams{Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	return periodicJobsFromRiverType(rows), nil
}

func (p *StandardPilot) PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobUpsertManyParams) ([]*PeriodicJob, error) {
	if !p.DurablePeriodicJobs.Enabled || len(params.Jobs) == 0 {
		return nil, nil
	}

	// Defensively filter out any IDless jobs, although the enqueuer is already
	// expected to do this.
	driverJobs := make([]riverdriver.PeriodicJobUpsertManyParamsJob, 0, len(params.Jobs))
	for _, job := range params.Jobs {
		if job.ID == "" {
			continue
		}
		driverJobs = append(driverJobs, riverdriver.PeriodicJobUpsertManyParamsJob{
			ID:        job.ID,
			NextRunAt: job.NextRunAt,
			UpdatedAt: job.UpdatedAt,
		})
	}
	if len(driverJobs) == 0 {
		return nil, nil
	}

	rows, err := exec.PeriodicJobUpsertMany(ctx, &riverdriver.PeriodicJobUpsertManyParams{
		Jobs:   driverJobs,
		Schema: params.Schema,
	})
	if err != nil {
		return nil, err
	}
	return periodicJobsFromRiverType(rows), nil
}

func (p *StandardPilot) PilotInit(archetype *baseservice.Archetype, params *PilotInitParams) {
	if archetype != nil {
		p.timeGen = archetype.Time
	}
}

func (p *StandardPilot) now() time.Time {
	if p.timeGen != nil {
		return p.timeGen.Now()
	}
	return time.Now()
}

func periodicJobsFromRiverType(rows []*rivertype.DurablePeriodicJob) []*PeriodicJob {
	if len(rows) == 0 {
		return nil
	}
	out := make([]*PeriodicJob, len(rows))
	for i, row := range rows {
		out[i] = &PeriodicJob{
			ID:        row.ID,
			CreatedAt: row.CreatedAt,
			NextRunAt: row.NextRunAt,
			UpdatedAt: row.UpdatedAt,
		}
	}
	return out
}

func (p *StandardPilot) ProducerInit(ctx context.Context, exec riverdriver.Executor, params *ProducerInitParams) (int64, ProducerState, error) {
	id := p.seq.Add(1)
	return id, &standardProducerState{}, nil
}

func (p *StandardPilot) ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error {
	return nil
}

func (p *StandardPilot) ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *ProducerShutdownParams) error {
	return nil
}

func (p *StandardPilot) QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, params *QueueMetadataChangedParams) error {
	return nil
}

type standardProducerState struct{}

func (s *standardProducerState) JobFinish(job *rivertype.JobRow) {
	// No-op
}

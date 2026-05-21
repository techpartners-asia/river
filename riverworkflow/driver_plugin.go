package riverworkflow

import (
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/riverworkflow/internal/workflowscheduler"
)

// workflowDriverPlugin wraps a [riverdriver.Driver] so River's driverPlugin
// machinery accepts it and uses [workflowPilot] as the pilot. The pilot
// returns a [workflowscheduler.WorkflowScheduler] as an additional
// leader-elected maintenance service.
type workflowDriverPlugin[TTx any] struct {
	riverdriver.Driver[TTx]
	pilot *workflowPilot
}

func (p *workflowDriverPlugin[TTx]) PluginInit(archetype *baseservice.Archetype) {
	p.pilot.archetype = archetype
}

func (p *workflowDriverPlugin[TTx]) PluginPilot() riverpilot.Pilot {
	return p.pilot
}

// workflowPilot is a Pilot that delegates all standard behavior to
// StandardPilot and injects the workflow scheduler as a leader-elected
// maintenance service via the pilotPlugin interface in the river package.
type workflowPilot struct {
	riverpilot.StandardPilot

	archetype *baseservice.Archetype
	exec      riverdriver.Executor
	schedCfg  *workflowscheduler.Config
}

func (p *workflowPilot) PluginMaintenanceServices() []startstop.Service {
	if p.archetype == nil || p.exec == nil {
		return nil
	}
	return []startstop.Service{
		workflowscheduler.New(p.archetype, p.schedCfg, p.exec),
	}
}

func (p *workflowPilot) PluginServices() []startstop.Service { return nil }

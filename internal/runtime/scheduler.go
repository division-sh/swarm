package runtime

import runtimepipeline "empireai/internal/runtime/pipeline"

type Schedule = runtimepipeline.Schedule

type Scheduler = runtimepipeline.Scheduler

func NewScheduler(callbacks ...func(Schedule)) *Scheduler {
	return runtimepipeline.NewScheduler(callbacks...)
}

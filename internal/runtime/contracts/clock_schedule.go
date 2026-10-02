package contracts

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule/cadence"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// FlowSchedule is an admitted clock declaration, not an armed activation.
type FlowSchedule struct {
	Cron  string `yaml:"cron,omitempty" json:"cron,omitempty"`
	Every string `yaml:"every,omitempty" json:"every,omitempty"`
	Emit  string `yaml:"emit" json:"emit"`
}

func (s FlowSchedule) Validate() error {
	if !eventidentity.IsValidName(s.Emit) {
		return fmt.Errorf("schedule emit must be an exact business event name")
	}
	if (s.Cron == "") == (s.Every == "") {
		return fmt.Errorf("schedule requires exactly one of cron or every")
	}
	if s.Cron != "" {
		_, err := cadence.ParseCron(s.Cron)
		return err
	}
	interval, err := time.ParseDuration(s.Every)
	if err != nil {
		return fmt.Errorf("invalid every duration: %w", err)
	}
	return cadence.ValidateEvery(interval)
}

func projectSchemaSchedulesValue(value yamlsource.Value) (map[string]FlowSchedule, error) {
	declarations, err := schemaValueDeclarations(value, true)
	if err != nil {
		return nil, err
	}
	out := make(map[string]FlowSchedule, len(declarations))
	for _, declaration := range declarations {
		fields, err := schemaValueFields(declaration.Value, "schedule", map[string]struct{}{"cron": {}, "every": {}, "emit": {}}, nil, true)
		if err != nil {
			return nil, err
		}
		_, cronPresent := fields["cron"]
		_, everyPresent := fields["every"]
		if cronPresent == everyPresent {
			return nil, nodeValueError(declaration.Value, fmt.Errorf("schedule requires exactly one of cron or every"))
		}
		var schedule FlowSchedule
		if err := schemaValueRequiredTexts(declaration.Value, fields, map[string]*string{"emit": &schedule.Emit}); err != nil {
			return nil, err
		}
		if err := schemaValueTexts(fields, map[string]*string{"cron": &schedule.Cron, "every": &schedule.Every}, true); err != nil {
			return nil, err
		}
		if err := schedule.Validate(); err != nil {
			return nil, nodeValueError(declaration.Value, err)
		}
		out[declaration.Name] = schedule
	}
	return out, nil
}

func validateClockScheduleTopology(bundle *WorkflowContractBundle) []error {
	var errs []error
	for _, view := range bundle.FlowViews() {
		if len(view.Schema.Schedules) == 0 {
			continue
		}
		for owner := bundle.FlowTree.ByID[view.Paths.FlowPath]; owner != nil; owner = owner.Parent {
			if !owner.Schema.Instance.Empty() {
				errs = append(errs, fmt.Errorf("flow %s schedules fire per run; per-instance cadence is not supported (keyed ancestor %s)", view.Paths.FlowPath, owner.Paths.FlowPath))
				break
			}
		}
	}
	return errs
}

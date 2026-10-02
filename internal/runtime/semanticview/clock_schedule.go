package semanticview

import (
	"sort"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// ClockSchedule is a source projection, not deployment or wakeup authority.
// Publication entry selection and offline describe consume the same facts.
type ClockSchedule struct {
	FlowID      string                        `json:"flow_id"`
	Name        string                        `json:"name"`
	Declaration runtimecontracts.FlowSchedule `json:"declaration"`
	SourceFile  string                        `json:"source_file"`
}

func ClockSchedules(source Source) []ClockSchedule {
	if source == nil {
		return nil
	}
	var out []ClockSchedule
	bundle, _ := Bundle(source)
	for flowID, schema := range source.FlowSchemaEntries() {
		file := ""
		if bundle != nil {
			file = bundle.FlowSources[flowID].Schema
		}
		for name, declaration := range schema.Schedules {
			out = append(out, ClockSchedule{FlowID: flowID, Name: name, Declaration: declaration, SourceFile: file})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FlowID != out[j].FlowID {
			return out[i].FlowID < out[j].FlowID
		}
		return out[i].Name < out[j].Name
	})
	return out
}

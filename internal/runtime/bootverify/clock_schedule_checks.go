package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkClockScheduleValidation(c *checkerContext) []Finding {
	var findings []Finding
	for _, schedule := range semanticview.ClockSchedules(c.source) {
		if message := clockScheduleContractFailure(c.source, schedule); message != "" {
			findings = append(findings, Finding{
				CheckID: "clock_schedule_validation", Severity: "error", Message: message,
				Location: schedule.SourceFile + ":schedules." + schedule.Name,
			})
		}
	}
	return findings
}

func clockScheduleContractFailure(source semanticview.Source, schedule semanticview.ClockSchedule) string {
	if err := schedule.Declaration.Validate(); err != nil {
		return err.Error()
	}
	ref := semanticview.ResolveFlowEventProof(source, schedule.FlowID, schedule.Declaration.Emit)
	if strings.HasPrefix(ref.Authored, "platform.") || runtimecontracts.PlatformEventCatalogContains(source.PlatformSpec(), ref.Canonical) {
		return "schedules emit ordinary instance business events, not platform-control events"
	}
	if !ref.HasSchema || !ref.IsAuthored(source) {
		return fmt.Sprintf("schedule %s emit %s requires an authored business event declaration", schedule.Name, schedule.Declaration.Emit)
	}
	schema, found, err := source.ResolveEffectiveCompiledFlowEventSchema(schedule.FlowID, schedule.Declaration.Emit)
	if err != nil || !found {
		return fmt.Sprintf("schedule %s emit %s requires an admitted event schema", schedule.Name, schedule.Declaration.Emit)
	}
	if len(schema.Fields()) != 0 {
		return fmt.Sprintf("schedule %s emits a bare event; %s declares payload fields", schedule.Name, schedule.Declaration.Emit)
	}
	if !timerFireEventHasConsumer(source, ref) {
		return fmt.Sprintf("schedule %s emit %s requires an actual consumer or selected-root output; add an explicit connect edge for a private child", schedule.Name, schedule.Declaration.Emit)
	}
	return ""
}

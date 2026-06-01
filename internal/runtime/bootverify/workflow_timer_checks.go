package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkTimerValidation(c *checkerContext) []Finding { return c.timerValidation() }

func (c *checkerContext) timerValidation() []Finding {
	if c.timerLoaded {
		return c.timerFindings
	}
	c.timerLoaded = true
	for _, timer := range c.source.WorkflowTimers() {
		owner := strings.TrimSpace(timer.Owner)
		if owner == "" {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s missing owner", timer.ID),
				Location: strings.TrimSpace(timer.ID),
			})
			continue
		}
		if owner != "runtime" {
			if _, systemNode := c.source.NodeEntries()[owner]; !systemNode {
				if !participantExistsLocal(c.source, owner) {
					c.timerFindings = append(c.timerFindings, Finding{
						CheckID:  "timer_validation",
						Severity: "error",
						Message:  fmt.Sprintf("timer %s owner %s missing from participants", timer.ID, owner),
						Location: strings.TrimSpace(timer.ID),
					})
				}
			}
		}
		if !eventExists(c.source, strings.TrimSpace(timer.Event)) {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s event %s missing from event catalog", timer.ID, timer.Event),
				Location: strings.TrimSpace(timer.ID),
			})
		}
		startTrigger, err := timeridentity.ParseStartTrigger(timer.StartOn)
		if err != nil {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s start_on %q is invalid: %v", timer.ID, timer.StartOn, err),
				Location: strings.TrimSpace(timer.ID),
			})
		} else {
			c.validateTimerTrigger(timer, "start_on", startTrigger)
		}
		cancelTrigger, err := timeridentity.ParseCancelTrigger(timer.CancelOn)
		if err != nil {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s cancel_on %q is invalid: %v", timer.ID, timer.CancelOn, err),
				Location: strings.TrimSpace(timer.ID),
			})
		} else {
			c.validateTimerTrigger(timer, "cancel_on", cancelTrigger)
		}
	}
	return c.timerFindings
}

func (c *checkerContext) validateTimerTrigger(timer runtimecontracts.WorkflowTimerContract, field string, trigger timeridentity.Trigger) {
	if !trigger.Valid() {
		return
	}
	switch trigger.Kind {
	case timeridentity.TriggerKindState:
		if !containsString(flowStatesForTimer(c.source, timer.FlowID), trigger.Name) {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "error",
				Message:  fmt.Sprintf("timer %s %s references unknown state %s", timer.ID, field, trigger.Name),
				Location: strings.TrimSpace(timer.ID),
			})
		}
	case timeridentity.TriggerKindEvent:
		if !eventExists(c.source, trigger.Name) {
			c.timerFindings = append(c.timerFindings, Finding{
				CheckID:  "timer_validation",
				Severity: "warning",
				Message:  fmt.Sprintf("timer %s %s references unknown event %s", timer.ID, field, trigger.Name),
				Location: strings.TrimSpace(timer.ID),
			})
		}
	}
}

func flowStatesForTimer(source semanticview.Source, flowID string) []string {
	flowID = strings.TrimSpace(flowID)
	if source == nil {
		return nil
	}
	if flowID != "" {
		return source.FlowStates(flowID)
	}
	stages := source.WorkflowStages()
	out := make([]string, 0, len(stages))
	for _, stage := range stages {
		name := strings.TrimSpace(stage.ID)
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

func participantExistsLocal(source semanticview.Source, participant string) bool {
	participant = strings.TrimSpace(participant)
	if participant == "" || source == nil {
		return false
	}
	if participant == "runtime" || participant == "human" {
		return true
	}
	if _, ok := source.NodeEntries()[participant]; ok {
		return true
	}
	for _, agent := range source.AgentEntries() {
		if strings.TrimSpace(agent.ID) == participant || strings.TrimSpace(agent.Role) == participant {
			return true
		}
	}
	return false
}

package bootverify

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkHandlerFieldCompliance(c *checkerContext) []Finding { return c.handlerFieldCompliance() }

func (c *checkerContext) handlerFieldCompliance() []Finding {
	if c.handlerLoaded {
		return c.handlerFindings
	}
	c.handlerLoaded = true
	for _, record := range c.source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		for eventType, handler := range c.source.ExecutableNodeEventHandlers(node) {
			eventType = strings.TrimSpace(eventType)
			for _, check := range handler.Guard.EffectiveChecks() {
				if strings.TrimSpace(check.Check) != "" || strings.TrimSpace(check.ID) == "" {
					continue
				}
				if guard, ok := c.source.GuardInstructionByID(check.ID); ok && !guard.Executable() {
					c.handlerFindings = append(c.handlerFindings, handlerFieldFinding(node.Key(), eventType,
						fmt.Sprintf("guard %s has no executable runtime implementation", check.ID)))
				}
			}
		}
	}
	c.handlerFindings = append(c.handlerFindings, runtimeHandledEventsMissingExecutors(c.source)...)
	return c.handlerFindings
}

func handlerFieldFinding(nodeID, eventContext, message string) Finding {
	return Finding{
		CheckID:  "handler_field_compliance",
		Severity: "error",
		Message:  fmt.Sprintf("node %s handler %s %s", nodeID, eventContext, message),
		Location: nodeID,
	}
}

func supportedWorkflowRuntimeExecutorIDs(source semanticview.Source) map[string]struct{} {
	out := map[string]struct{}{}
	if source == nil {
		return out
	}
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		if len(source.ExecutableNodeEventHandlers(node)) > 0 || len(record.Entry.EventHandlers) > 0 {
			out[node.Key()] = struct{}{}
		}
	}
	return out
}

func firstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

package bootverify

import (
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func checkHandlerFieldCompliance(c *checkerContext) []Finding { return c.handlerFieldCompliance() }

func (c *checkerContext) handlerFieldCompliance() []Finding {
	if c.handlerLoaded {
		return c.handlerFindings
	}
	c.handlerLoaded = true
	c.handlerFindings = append(c.handlerFindings, runtimeHandledEventsMissingExecutors(c.source)...)
	return c.handlerFindings
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

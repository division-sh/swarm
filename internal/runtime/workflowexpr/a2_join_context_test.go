package workflowexpr

import (
	"strings"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestA2JoinClosureContextTypes(t *testing.T) {
	for _, context := range []JoinContext{JoinContextArrival, JoinContextCountArrival} {
		options := ValueExpressionOptions{AllowJoin: true, JoinContext: context, JoinResultType: rc.CatalogTypeReference{Type: "integer"}}
		for _, expression := range []string{`join.close_reason == "until"`, "join.completed <= join.expected", "join.results.exists(r, r > 0)", "!join.timed_out", "loop.attempt > 0"} {
			if err := ValidateValueExpressionWithOptions(expression, options); err != nil {
				t.Fatalf("context %v lost %s: %v", context, expression, err)
			}
		}
		for _, expression := range []string{"join.close_reason > 1", "payload.result", "event.id", "_entity.current_state"} {
			if err := ValidateValueExpressionWithOptions(expression, options); err == nil {
				t.Fatalf("context %v admitted %s", context, expression)
			}
		}
		err := ValidateValueExpressionWithOptions("size(join.missing)", options)
		if context == JoinContextArrival && err != nil {
			t.Fatalf("explicit-membership missing context erased: %v", err)
		}
		if context == JoinContextCountArrival && (err == nil || !strings.Contains(err.Error(), "unsupported join.missing")) {
			t.Fatalf("count fabricated missing member identities: %v", err)
		}
	}
	if err := ValidateValueExpressionWithOptions("join.close_reason", ValueExpressionOptions{AllowJoin: true, JoinContext: JoinContextFanOutDelivery}); err == nil {
		t.Fatal("all-settled fan-out acquired arrival closure context")
	}
}

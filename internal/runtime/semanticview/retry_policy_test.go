package semanticview

import (
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestHandlerRetryBaseUsesOneCanonicalPolicyProjection(t *testing.T) {
	if got := HandlerRetryBase(nil); got != time.Second {
		t.Fatalf("default handler retry base = %s, want 1s", got)
	}
	for _, test := range []struct {
		name  string
		value any
		want  time.Duration
	}{
		{name: "integer", value: 17, want: 17 * time.Second},
		{name: "decimal", value: 2.5, want: 2500 * time.Millisecond},
		{name: "numeric string", value: "7", want: 7 * time.Second},
		{name: "invalid", value: "later", want: time.Second},
		{name: "non-positive", value: 0, want: time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := Wrap(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
				handlerRetryBasePolicyKey: {Value: test.value},
			}}})
			if got := HandlerRetryBase(source); got != test.want {
				t.Fatalf("handler retry base = %s, want %s", got, test.want)
			}
		})
	}
}

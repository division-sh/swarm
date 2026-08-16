package runfork

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestRunForkContractFrontierRecipientUsesPrivateTypedWireCodec(t *testing.T) {
	want := NewRunForkContractFrontierRecipient(
		events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "review", "worker")), "review/inst-1", "compiled_connect_evaluation", agentidentity.Identity{},
	)
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got RunForkContractFrontierRecipient
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Recipient != want.Recipient || got.Path != want.Path || got.RouteSourceCode() != want.RouteSourceCode() {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}

	for _, hostile := range []string{
		`{"subscriber_type":"platform","subscriber_id":"worker"}`,
		`{"subscriber_type":"node","subscriber_id":"worker","unknown":true}`,
	} {
		if err := json.Unmarshal([]byte(hostile), &got); err == nil {
			t.Fatalf("Unmarshal(%s) succeeded, want fail-closed private codec", hostile)
		}
	}
	if _, err := json.Marshal(RunForkContractFrontierRecipient{}); err == nil || !strings.Contains(err.Error(), "recipient is required") {
		t.Fatalf("Marshal(zero) error = %v, want required recipient", err)
	}
}

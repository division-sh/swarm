package manager

import (
	"context"
	"strings"
	"testing"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func TestAgentManagerControlAdmissionRejectsDifferentSourceArtifactFact(t *testing.T) {
	const runtimeID = "11111111-1111-4111-8111-111111111111"
	const ownedHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	owned := managerSourceArtifactFact(t, ownedHash)
	base := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(runtimeID, ownedHash))
	base = runtimecorrelation.WithRuntimeInstanceID(base, runtimeID)
	base = runtimecorrelation.WithSourceArtifactFact(base, owned)

	for name, contextFact := range map[string]runtimecorrelation.SourceArtifactFact{
		"different hash": managerSourceArtifactFact(t, "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
	} {
		t.Run(name, func(t *testing.T) {
			am := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{BaseContext: base})
			ctx := runtimecorrelation.WithSourceArtifactFact(context.Background(), contextFact)

			_, err := am.SendDirective(ctx, runtimeagentcontrol.SendDirectiveRequest{
				AgentID:   "agent-a",
				Directive: "inspect",
			})
			if err == nil || !strings.Contains(err.Error(), "bundle source fact conflicts") {
				t.Fatalf("SendDirective error = %v, want bundle source conflict before control mutation", err)
			}
		})
	}
}

func managerSourceArtifactFact(t *testing.T, bundleHash string) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		t.Fatalf("construct bundle source fact: %v", err)
	}
	return fact
}

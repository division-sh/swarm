package manager

import (
	"context"
	"strings"
	"testing"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func TestAgentManagerControlAdmissionRejectsNonExactBundleSourceFact(t *testing.T) {
	const runtimeID = "11111111-1111-4111-8111-111111111111"
	const ownedHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	owned := managerBundleSourceFact(t, ownedHash, true)
	base := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(runtimeID, ownedHash))
	base = runtimecorrelation.WithRuntimeInstanceID(base, runtimeID)
	base = runtimecorrelation.WithBundleSourceFact(base, owned)

	for name, contextFact := range map[string]runtimecorrelation.BundleSourceFact{
		"different hash":             managerBundleSourceFact(t, "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", true),
		"same hash different source": managerBundleSourceFact(t, ownedHash, false),
	} {
		t.Run(name, func(t *testing.T) {
			am := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{BaseContext: base})
			ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), contextFact)

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

func managerBundleSourceFact(t *testing.T, bundleHash string, persisted bool) runtimecorrelation.BundleSourceFact {
	t.Helper()
	var (
		fact runtimecorrelation.BundleSourceFact
		err  error
	)
	if persisted {
		fact, err = runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
	} else {
		fact, err = runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)
	}
	if err != nil {
		t.Fatalf("construct bundle source fact: %v", err)
	}
	return fact
}

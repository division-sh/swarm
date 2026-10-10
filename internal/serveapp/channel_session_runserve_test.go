//go:build linux || darwin

package serveapp

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestRunServeWhatsAppUnpairedDeclarationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			opts := cliapp.ServeOptions{
				SourceRoot:       filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session"),
				PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
				SelfCheck: true, AbandonActiveRuns: true, Verbose: true,
				WorkspaceBackend: "host", WorkspaceBackendSet: true, StoreMode: backend, StoreModeSet: true,
			}
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
					filepath.Join(t.TempDir(), "native-serve.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				dsn := testutil.StartEmptyPostgresDSN(t)
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
			}
			for range 2 {
				process := startServeRuntimeTestProcess(t, opts)
				process.waitForReadyLine()
				process.mu.Lock()
				owner := process.runtime
				process.mu.Unlock()
				if owner == nil {
					t.Fatal("RunServe did not install the actual runtime")
				}
				targets, err := owner.PlanStandingTargets()
				if err != nil || len(targets) != 0 {
					t.Fatal("unpaired declaration granted a business target", targets, err)
				}
				candidates, err := owner.PlanStandingServiceCandidates()
				if err != nil || len(candidates) != 1 || candidates[0].BindingEnabled {
					t.Fatal("unpaired declaration lost discovery or enabled standing retention", candidates, err)
				}
				endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
				var listed struct {
					PrincipalID string                                       `json:"principal_id"`
					Channels    []channelonboarding.ConnectedChannelReadback `json:"channels"`
				}
				requireServedJSONRPCResult(t, endpoint, "channel.list", map[string]any{}, &listed)
				found := 0
				for _, channel := range listed.Channels {
					if channel.Identity.Interface.ChannelPackID == "provider.whatsapp.hitl_channel" {
						found++
						if channel.Identity.Status != "unbound" || channel.Identity.PrincipalID != listed.PrincipalID || listed.PrincipalID == "" {
							t.Fatal("declaration readback invented account or operator binding", channel)
						}
					}
				}
				if found != 1 {
					t.Fatal("public discovery lost the one installed WhatsApp interface", listed)
				}
				if code := process.stop(); code != 0 {
					t.Fatal("ordinary unpaired shutdown failed", code, process.outputString())
				}
				output := process.outputString()
				if !strings.Contains(output, "session admission required") || !strings.Contains(output, "swarm channel connect") {
					t.Fatal("dormant native source lacks actionable onboarding guidance", output)
				}
				if strings.Contains(output, "whatsapp webhook") || strings.Contains(output, "declared ingress is local only") {
					t.Fatal("native session was presented as a public webhook", output)
				}
			}
		})
	}
}

func TestNativeSessionPresentationNeverSuggestsWebhookExposure(t *testing.T) {
	native := serveLifecycleIngressFact{Provider: "whatsapp", Alias: "native", Subject: packs.Subject{
		TriggerAdmission: &packs.TriggerAdmission{Transport: packs.ChannelTransportSession}}}
	webhook := serveLifecycleIngressFact{Provider: "telegram", Alias: "hooks", Subject: packs.Subject{
		TriggerAdmission: &packs.TriggerAdmission{Transport: packs.ChannelTransportWebhook}}}
	for _, exposed := range []bool{false, true} {
		facts := publicIngressPresentation([]serveLifecycleIngressFact{native, webhook}, publicingress.Snapshot{
			PublicIngressEnabled: exposed,
			Exposure:             &publicingress.ExposureEvidence{PublicOrigin: "https://ingress.example.test"},
		})
		if facts[0].URL != "in-process session" || requiresPublicIngressPresentation(facts[:1]) || !requiresPublicIngressPresentation(facts) {
			t.Fatal("native transport inherited webhook semantics", facts)
		}
		want := "not exposed"
		if exposed {
			want = "https://ingress.example.test/webhooks/hooks/telegram"
		}
		if facts[1].URL != want {
			t.Fatal("webhook exposure presentation changed", facts[1])
		}
	}
}

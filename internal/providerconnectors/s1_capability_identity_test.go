package providerconnectors

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func s1MalformedCapabilityToolIDs() []string {
	return []string{
		"telegram.send_message ", "telegram. send_message", "telegram.",
		" telegram.send_message", "TEAM.send", "_team.send", "con.send",
		"team-bot.send_message ", "team_bot. send_message", "team..send",
	}
}

func TestS1ConnectorCapabilityReadbackRejectsMalformedToolIdentity(t *testing.T) {
	registry := testPackRegistry(t)
	for _, toolID := range s1MalformedCapabilityToolIDs() {
		t.Run(toolID, func(t *testing.T) {
			for _, withValidPeer := range []bool{false, true} {
				tools := map[string]runtimecontracts.ToolSchemaEntry{toolID: telegramConnectorTool("https://api.telegram.org")}
				if withValidPeer {
					tools["telegram.send_message"] = telegramConnectorTool("https://api.telegram.org")
				}
				source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: tools})
				if len(ValidateSource(source)) == 0 {
					t.Fatal("negative fixture must fail the existing tool-ID validator")
				}
				for _, includeInstalled := range []bool{false, true} {
					subjects, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{Registry: registry, IncludeInstalled: includeInstalled})
					if err == nil || !strings.Contains(err.Error(), toolID) || !strings.Contains(err.Error(), "<provider>.<action>") || len(subjects) != 0 {
						t.Errorf("malformed identity gained readback (peer=%t installed=%t): subjects=%+v err=%v", withValidPeer, includeInstalled, subjects, err)
					}
				}
			}
		})
	}
}

type s1CapabilityCredentialObserver struct {
	runtimecredentials.Store
	reads int
	err   error
}

func (s *s1CapabilityCredentialObserver) Get(context.Context, string) (string, bool, error) {
	s.reads++
	return "", false, s.err
}

func TestS1ConnectorCapabilityIdentityRefusesBeforeCredentialRead(t *testing.T) {
	registry := testPackRegistry(t)
	for _, toolID := range s1MalformedCapabilityToolIDs() {
		t.Run(toolID, func(t *testing.T) {
			credentialErr := errors.New("credential observation reached")
			store := &s1CapabilityCredentialObserver{err: credentialErr}
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
				toolID: telegramConnectorTool("https://api.telegram.org"),
			}})
			subjects, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{Registry: registry, StaticCredentials: store})
			if err == nil || errors.Is(err, credentialErr) || store.reads != 0 || len(subjects) != 0 {
				t.Fatalf("invalid identity reached credential evaluation: reads=%d subjects=%+v err=%v", store.reads, subjects, err)
			}
		})
	}
	store := &s1CapabilityCredentialObserver{err: errors.New("valid identity reached credentials")}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"telegram.send_message": telegramConnectorTool("https://api.telegram.org"),
	}})
	if subjects, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{Registry: registry, StaticCredentials: store}); !errors.Is(err, store.err) || store.reads != 1 || len(subjects) != 0 {
		t.Fatalf("positive did not exercise credential observation: reads=%d subjects=%+v err=%v", store.reads, subjects, err)
	}
}

func TestS1ConnectorInstalledCapabilityRejectsMalformedToolIdentity(t *testing.T) {
	for _, toolID := range s1MalformedCapabilityToolIDs() {
		t.Run(toolID, func(t *testing.T) {
			pack := s1ConnectorPack("telegram")
			tool := telegramConnectorTool("https://api.telegram.org")
			pack.Manifest.Tools = map[string]runtimecontracts.ToolSchemaEntry{toolID: tool}
			if _, err := NewPackRegistry(pack); err == nil {
				t.Fatal("installed registry admitted malformed identity")
			}
			subject, err := availableCapabilitySubject(InstalledTool{Provider: "telegram", ToolID: toolID, Tool: tool, Pack: pack})
			if err == nil || !strings.Contains(err.Error(), toolID) || !strings.Contains(err.Error(), "<provider>.<action>") || subject.ID != "" {
				t.Fatalf("installed reader repaired malformed identity: subject=%+v err=%v", subject, err)
			}
		})
	}
}

func TestS1ConnectorCapabilityReadbackPreservesExactInstalledAndEffectiveIdentity(t *testing.T) {
	registry, err := NewPackRegistry(s1ConnectorPack("team-bot"), s1ConnectorPack("team_bot"))
	if err != nil {
		t.Fatal(err)
	}
	for _, effectiveKey := range []string{"team-bot", "team_bot"} {
		t.Run(effectiveKey, func(t *testing.T) {
			toolID := effectiveKey + ".send_message"
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
				toolID: telegramConnectorTool("https://api.telegram.org"),
			}})
			if errs := ValidateSource(source); len(errs) != 0 {
				t.Fatalf("positive validator: %v", errs)
			}
			subjects, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{Registry: registry, IncludeInstalled: true})
			if err != nil || len(subjects) != 2 {
				t.Fatalf("readback: subjects=%+v err=%v", subjects, err)
			}
			seen := map[string]bool{}
			for _, subject := range subjects {
				if seen[subject.Provider] || (subject.Provider != "team-bot" && subject.Provider != "team_bot") || subject.ID != subject.Provider+".send_message" || subject.Action != "send_message" {
					t.Fatalf("identity changed or duplicated: %+v", subjects)
				}
				seen[subject.Provider] = true
				wantApplicability, wantStatus := "installed", packs.StatusAvailable
				if subject.Provider == effectiveKey {
					wantApplicability, wantStatus = "effective", packs.StatusNotReady
				}
				if subject.Applicability != wantApplicability || subject.Status != wantStatus {
					t.Fatalf("effective identity replaced a different installed row: %+v", subject)
				}
			}
		})
	}
}

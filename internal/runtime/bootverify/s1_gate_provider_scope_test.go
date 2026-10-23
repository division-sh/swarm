package bootverify

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1HumanGateRequiresScopedChannelDirectory(t *testing.T) {
	for _, tc := range []struct {
		name      string
		gateFlow  string
		channels  []string
		triggers  []string
		wantError bool
	}{
		{name: "missing", gateFlow: ".", wantError: true},
		{name: "local", gateFlow: ".", channels: []string{"team"}},
		{name: "ancestor", gateFlow: "child/deep", channels: []string{"team"}},
		{name: "nearest", gateFlow: "child", channels: []string{"team", "child/team"}},
		{name: "sibling_not_visible", gateFlow: "child", channels: []string{"sibling/team"}, wantError: true},
		{name: "descendant_not_visible", gateFlow: ".", channels: []string{"child/team"}, wantError: true},
		{name: "no_role_fallback", gateFlow: "child", channels: []string{"team"}, triggers: []string{"child/team"}, wantError: true},
		{name: "different_key_remains_visible", gateFlow: "child", channels: []string{"team"}, triggers: []string{"child/other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "stages: []\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, tc.gateFlow, "schema.yaml"), "stages:\n  review:\n    gate:\n      decision: approve\n      outcomes:\n        accept: {advances_to: done}\n  done: {final: true}\n")
			for _, directory := range tc.channels {
				writeS1ScopeProvider(t, root, directory, true)
			}
			for _, directory := range tc.triggers {
				writeS1ScopeProvider(t, root, directory, false)
			}
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(repoRootForBootverifyTest(t), sourceartifactfixture.CapturedRoot(t, artifact), "", contracts.WorkflowContractLoadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if gates := bundle.Semantics.Gates; len(gates) != 1 || gates[0].FlowID != tc.gateFlow {
				t.Fatalf("fixture did not compile its exact gate: %+v", gates)
			}
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			for _, purpose := range []ValidationPurpose{StructuralValidation, ExecutionValidation} {
				report := Run(context.Background(), semanticview.Wrap(bundle), Options{Purpose: purpose})
				var observations int
				for _, observation := range report.Observations {
					if observation.CheckID != "stage_gate_channel_scope" {
						continue
					}
					observations++
					want := AdmissionPassed
					if tc.wantError {
						want = AdmissionFailed
					}
					if observation.Class != AdmissionSourceObservation || observation.Status != want {
						t.Fatalf("scope check did not execute as a source observation: %+v", observation)
					}
				}
				if observations != 1 {
					t.Fatalf("scope check observation count=%d want 1", observations)
				}
				var scopeFindings []Finding
				for _, finding := range report.Findings {
					if finding.CheckID == "stage_gate_channel_scope" {
						scopeFindings = append(scopeFindings, finding)
					}
					if finding.CheckID == stageGateValidationCheckID {
						t.Fatalf("fixture has an invalid gate: %+v", finding)
					}
				}
				if !tc.wantError {
					if len(scopeFindings) != 0 {
						t.Fatalf("visible channel directory refused: %+v", scopeFindings)
					}
					continue
				}
				if len(scopeFindings) != 1 {
					t.Fatalf("human gate without a visible channel: findings=%+v, want one structural refusal", scopeFindings)
				}
				finding := scopeFindings[0]
				locationFlow := tc.gateFlow
				if locationFlow == "." {
					locationFlow = "root"
				}
				if finding.Severity != SeverityHardInvalidity || finding.Location != fmt.Sprintf("flow %s stage review gate approve", locationFlow) || !strings.Contains(finding.Remediation, "swarm import telegram") {
					t.Fatalf("channel-scope refusal lost severity, owning flow or next command: %+v", finding)
				}
			}
		})
	}
}

// Body vocabulary is checked by its role owner; this fixture proves structural visibility only.
func writeS1ScopeProvider(t *testing.T, root, directory string, channel bool) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, directory, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [message]}, cannot: [execute code]}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, directory, "trigger.yaml"), "{}\n")
	if channel {
		writeBootverifyFixtureFile(t, filepath.Join(root, directory, "connector.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, directory, "channel.yaml"), "{}\n")
	}
}

func TestS1HumanGateCannotBorrowProgrammaticProviderVisibility(t *testing.T) {
	bundle := &contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{
		Gates: []contracts.WorkflowGatePlan{{FlowID: ".", Stage: "review", Decision: "approve"}},
	}}
	findings := checkStageGateChannelScope(&checkerContext{source: semanticview.Wrap(bundle)})
	if len(findings) != 1 || findings[0].CheckID != stageGateChannelScopeCheckID || findings[0].Severity != SeverityHardInvalidity || !strings.Contains(findings[0].Message, "cannot establish") {
		t.Fatalf("programmatic gate bypassed rooted membership: %+v", findings)
	}
	bundle.Semantics.Gates = nil
	if findings := checkStageGateChannelScope(&checkerContext{source: semanticview.Wrap(bundle)}); len(findings) != 0 {
		t.Fatalf("gate-free source acquired a channel requirement: %+v", findings)
	}
}

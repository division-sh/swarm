package pipeline

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func Test2376ActivityContractPinsConsumeAdmittedSource(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo,
		canonicalrouting.ExampleRoot(t, canonicalrouting.TemplateCreateMintedKey), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(identity.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	bus := &recordingPipelineBus{}
	pc := newPreviewPipelineCoordinatorForTest(bus, PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source}, SourceArtifactFact: fact,
	})
	parent := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	for _, row := range []struct {
		name, hash, version, failure string
	}{
		{"target", identity.BundleHash, identity.WorkflowVersion, ""},
		{"parent_hash", parent, identity.WorkflowVersion, "activity_contract_pin_unavailable"},
		{"parent_version", identity.BundleHash, parent, "activity_contract_pin_unavailable"},
		{"parent_both", parent, parent, "activity_contract_pin_unavailable"},
		{"hash_only", identity.BundleHash, "", "activity_contract_pin_incomplete"},
		{"version_only", "", identity.WorkflowVersion, "activity_contract_pin_incomplete"},
	} {
		t.Run(row.name, func(t *testing.T) {
			intent := testActivityIntent("https://example.com/source")
			intent.BundleHash, intent.WorkflowVersion = row.hash, row.version
			failure := (pipelineActivityDispatcher{coordinator: pc}).activityContractPinFailure(context.Background(), intent, source)
			if row.failure == "" {
				if failure != nil {
					t.Fatalf("admitted target refused: %#v", failure)
				}
				return
			}
			if failure == nil || failure.Detail.Code != row.failure {
				t.Fatalf("pin failure = %#v, want %s", failure, row.failure)
			}
			request, err := activityRequestEmitIntent(intent)
			if err != nil {
				t.Fatal(err)
			}
			handled, outcome, err := pc.handleActivityRequestEvent(testAuthorActivityContext(t, context.Background()), request.Event)
			if row.failure == "activity_contract_pin_incomplete" {
				if !handled || err == nil || !strings.Contains(err.Error(), "incomplete contract pin") || bus.outboxCount() != 0 {
					t.Fatalf("incomplete request admission: handled=%t err=%v", handled, err)
				}
				return
			}
			if err != nil || !handled {
				t.Fatalf("dispatch refusal: handled=%t err=%v", handled, err)
			}
			retry, ok := outcome.RetryRelease()
			if !ok || retry.ReasonCode() != row.failure {
				t.Fatalf("dispatch did not release claim for exact pin refusal: %#v", outcome)
			}
			if _, durable := outcome.Disposition(); durable || bus.outboxCount() != 0 {
				t.Fatal("refused source pin committed activity work")
			}
		})
	}
}

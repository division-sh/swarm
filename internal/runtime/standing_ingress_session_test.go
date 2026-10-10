package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

type standingSessionBindingProbe struct {
	standingLearnedCredentialStore
	current map[string]bool
	checks  int
}

func (s *standingSessionBindingProbe) SessionStandingBindingCurrent(ctx context.Context, op channelonboarding.Operation) (bool, error) {
	s.checks++
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return s.current[op.OperationID], s.err
}

func standingSessionRuntimeFixture(t *testing.T) (*Runtime, *standingSessionBindingProbe) {
	t.Helper()
	repo := filepath.Join("..", "..")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, filepath.Join(repo, "internal/serveapp/testdata/whatsapp-session"),
		runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	plan := packfixture.ChannelPlanByID(t, projection.ChannelPlans, "provider.whatsapp.hitl_channel")
	identity, err := plan.InterfaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	op := channelonboarding.Operation{OperationID: uuid.NewString(), Revision: 2, BindingRevision: 1,
		Posture: channelonboarding.ActivationSessionConnection, Phase: channelonboarding.PhaseAwaitingOperatorConfirmation,
		Provider: "whatsapp", Interface: identity, TargetSelector: "ingress:.:whatsapp",
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: runtimeContextTestHashA,
			PackInventoryGeneration: bundle.PackInventory.Digest(), PlanGeneration: generation}}
	store := &standingSessionBindingProbe{standingLearnedCredentialStore: standingLearnedCredentialStore{operations: []channelonboarding.Operation{op}},
		current: map[string]bool{op.OperationID: true}}
	rt := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)},
		ProviderTriggerCatalog: projection.ProviderTriggers, ChannelPlans: []packs.SatisfactionPlan{plan}, ChannelOnboardingStore: store,
		SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA)}}
	return rt, store
}

func TestStandingSessionPlanRequiresConfirmedSelectedBinding(t *testing.T) {
	for _, row := range []string{"current", "not_confirmed", "missing", "foreign_source", "foreign_inventory", "foreign_plan", "foreign_interface", "foreign_selector", "foreign_provider", "duplicate", "read_error"} {
		t.Run(row, func(t *testing.T) {
			rt, store := standingSessionRuntimeFixture(t)
			op := &store.operations[0]
			switch row {
			case "not_confirmed":
				store.current[op.OperationID] = false
			case "missing":
				store.operations = nil
			case "foreign_source":
				op.Coordinate.BundleHash = runtimeContextTestHashB
			case "foreign_inventory":
				op.Coordinate.PackInventoryGeneration += "other"
			case "foreign_plan":
				op.Coordinate.PlanGeneration, _ = plangeneration.FromCanonicalValue(map[string]string{"other": "plan"})
			case "foreign_interface":
				op.Interface.ChannelManifestHash += "other"
			case "foreign_selector":
				op.TargetSelector = "ingress:other:whatsapp"
			case "foreign_provider":
				op.Provider = "telegram"
			case "duplicate":
				other := *op
				other.OperationID = uuid.NewString()
				store.current[other.OperationID] = true
				store.operations = append(store.operations, other)
			case "read_error":
				store.err = errors.New("selected binding observation failed")
			}
			targets, err := rt.PlanStandingTargets()
			if row == "duplicate" || row == "read_error" {
				if err == nil {
					t.Fatal("ambiguous/unobserved authority enabled standing ingress", targets)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if row == "current" {
				if len(targets) != 1 || targets[0].ServiceID != runtimeflowidentity.StandingServiceID(".") || targets[0].RunID != "" {
					t.Fatal("confirmed responsibility did not plan its unmaterialized target", targets)
				}
				return
			}
			candidates, err := rt.PlanStandingServiceCandidates()
			if err != nil || len(targets) != 0 || len(candidates) != 1 || candidates[0].BindingEnabled ||
				candidates[0].BindingBlockReason != runtimerunlifecycle.StandingBindingSessionRequired {
				t.Fatal("non-current declaration created executable/retaining authority", targets, candidates, err)
			}
		})
	}
}

func TestStandingSessionFrozenBindingDoesNotAdoptReplacement(t *testing.T) {
	rt, store := standingSessionRuntimeFixture(t)
	if _, err := rt.PlanStandingTargets(); err != nil {
		t.Fatal(err)
	}
	if err := rt.ValidateStandingIngressCredentials(context.Background()); err != nil {
		t.Fatal(err)
	}
	original := store.operations[0]
	store.current[original.OperationID] = false
	replacement := original
	replacement.OperationID = uuid.NewString()
	store.current[replacement.OperationID] = true
	store.operations = []channelonboarding.Operation{replacement}
	if err := rt.ValidateStandingIngressCredentials(context.Background()); err == nil {
		t.Fatal("frozen binding adopted a later responsibility")
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := rt.ValidateStandingIngressCredentials(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled selected observation granted admission", err)
	}
}

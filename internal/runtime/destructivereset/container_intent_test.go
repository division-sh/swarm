package destructivereset

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestManagedContainerReplayNeverSelectsSameNameSameLabelsSuccessor(t *testing.T) {
	predecessor := managedInspection("reused-name", "agent", true, true)
	predecessor.RuntimeID = "predecessor-object"
	planned := ContainerRefFromIdentity(predecessor.Identity, predecessor.RuntimeID, ContainerActionStop)
	raw, err := json.Marshal(planned)
	if err != nil {
		t.Fatal(err)
	}
	var restored ContainerRef
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	successor := predecessor
	successor.RuntimeID = "successor-object"
	runtime := &recordingManagedContainerRuntime{inspections: map[string]ManagedContainerInspection{
		"reused-name": successor, "successor-object": successor,
	}}
	now := time.Now().UTC()
	result, err := (ManagedContainerStopper{Runtime: runtime}).Apply(context.Background(), ContainerResetRequest{
		ActorTokenID: "operator",
		Result: Result{OperationName: DefaultOperationName, PlannedAt: now,
			Plan: Plan{ManagedContainers: []ContainerRef{restored}}},
		Cleanup: CleanupResult{OperationName: DefaultOperationName, AppliedAt: now},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.inspected) != 1 || runtime.inspected[0] != predecessor.RuntimeID || len(runtime.stops) != 0 ||
		len(result.Missing) != 1 || result.Missing[0].RuntimeID != predecessor.RuntimeID {
		t.Fatalf("historical container intent selected successor: calls=%v stops=%v result=%+v", runtime.inspected, runtime.stops, result)
	}
}

func TestManagedContainerMissingImmutableIntentFailsBeforeInspection(t *testing.T) {
	identity := managedInspection("named-only", "agent", true, true).Identity
	runtime := &recordingManagedContainerRuntime{}
	now := time.Now().UTC()
	_, err := (ManagedContainerStopper{Runtime: runtime}).Apply(context.Background(), ContainerResetRequest{
		ActorTokenID: "operator",
		Result: Result{OperationName: DefaultOperationName, PlannedAt: now, DryRun: true,
			Plan: Plan{ManagedContainers: []ContainerRef{ContainerRefFromIdentity(identity, "", ContainerActionStop)}}},
	})
	if !errors.Is(err, ErrInvalidRequest) || len(runtime.inspected) != 0 || len(runtime.stops) != 0 {
		t.Fatalf("missing intent used a name fallback: err=%v calls=%v stops=%v", err, runtime.inspected, runtime.stops)
	}
}

func TestContainerSettlementRequiresExactCompleteTargetAccounting(t *testing.T) {
	inspection := managedInspection("owned", "agent", true, true)
	planned := ContainerRefFromIdentity(inspection.Identity, inspection.RuntimeID, ContainerActionStop)
	valid := ContainerResetResult{Selected: []ContainerRef{planned}, Stopped: []ContainerRef{planned}}
	if err := validateContainerSettlement([]ContainerRef{planned}, valid); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ContainerResetResult){
		"missing receipt":   func(r *ContainerResetResult) { r.Stopped = nil },
		"duplicate receipt": func(r *ContainerResetResult) { r.Stopped = append(r.Stopped, planned) },
		"foreign object":    func(r *ContainerResetResult) { r.Stopped[0].RuntimeID = "foreign" },
		"foreign projection": func(r *ContainerResetResult) {
			r.Stopped[0].SourceProjection = "runtime-projection-v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		},
		"contradictory disposition": func(r *ContainerResetResult) {
			r.Missing = []ContainerRef{withContainerAction(planned, ContainerActionMissing)}
		},
		"preserved is not settled": func(r *ContainerResetResult) { r.Preserved = []ContainerRef{planned} },
		"unselected stop":          func(r *ContainerResetResult) { r.Selected = nil },
	} {
		t.Run(name, func(t *testing.T) {
			result := copyContainerResetResult(valid)
			mutate(&result)
			if err := validateContainerSettlement([]ContainerRef{planned}, result); err == nil {
				t.Fatal("invalid settlement accepted")
			}
		})
	}
}

func TestDurableContainerIntentRejectsMalformedIdentityBeforeEffects(t *testing.T) {
	inspection := managedInspection("owned", "agent", true, true)
	valid := ContainerRefFromIdentity(inspection.Identity, inspection.RuntimeID, ContainerActionStop)
	for name, change := range map[string]func([]ContainerRef) []ContainerRef{
		"missing immutable id":   func(refs []ContainerRef) []ContainerRef { refs[0].RuntimeID = ""; return refs },
		"duplicate immutable id": func(refs []ContainerRef) []ContainerRef { return append(refs, refs[0]) },
		"missing source":         func(refs []ContainerRef) []ContainerRef { refs[0].BundleHash = ""; return refs },
		"missing projection":     func(refs []ContainerRef) []ContainerRef { refs[0].SourceProjection = ""; return refs },
		"foreign owner":          func(refs []ContainerRef) []ContainerRef { refs[0].Owner = "foreign"; return refs },
		"not eligible":           func(refs []ContainerRef) []ContainerRef { refs[0].ResetEligible = false; return refs },
		"wrong action":           func(refs []ContainerRef) []ContainerRef { refs[0].Action = ContainerActionMissing; return refs },
	} {
		t.Run(name, func(t *testing.T) {
			lifecycle := &recordingResetLifecycle{}
			c := &Coordinator{
				Planner: plannerFunc(func(_ context.Context, req Request) (Plan, error) {
					return Plan{CleanupRunSetKnown: true, IncludeSourceArtifacts: req.IncludeSourceArtifacts,
						ManagedContainers: change([]ContainerRef{valid})}, nil
				}),
				Locks: &recordingLockManager{acquired: true}, RuntimeContexts: lifecycle,
				Quiescer: quiescenceApplierFunc(func(context.Context, QuiescenceRequest) (QuiescenceResult, error) {
					t.Error("malformed intent reached quiescence")
					return QuiescenceResult{}, errors.New("unexpected quiescence")
				}),
				Cleaner: successfulCleaner(), Containers: successfulContainers(),
			}
			journal := installCoordinatorOperationFixture(t, c)
			_, err := c.Execute(context.Background(), Request{OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "hash"})
			if err == nil || journal.operation.Phase != PhaseAdmitted {
				t.Fatalf("malformed intent advanced: phase=%s err=%v", journal.operation.Phase, err)
			}
		})
	}
}

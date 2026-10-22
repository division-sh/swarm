package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privatefork "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestRunStartRequiresExactNativeCreationAttempt(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		runID, fact := uuid.NewString(), runAdmissionFact(t, "a")
		withRunAdmissionAttempt(t, dialect, context.Background(), func(ctx context.Context, attempt *Attempt, tx *sql.Tx) {
			for _, invalid := range []context.Context{context.Background(), context.WithValue(ctx, sqlAttemptKey{}, "not-native")} {
				if err := attempt.DeclareRunStart(invalid, runID, fact.BundleHash(), ""); err == nil || attempt.effects.HasDeclarations() {
					t.Fatal("unbound context minted start evidence")
				}
			}
			if err := attempt.DeclareRunStart(ctx, runID, fact.BundleHash(), ""); err != nil {
				t.Fatal(err)
			}
			before := reflect.ValueOf(attempt.effects).Pointer()
			ref, err := privatefork.NewFactRef(privatefork.FamilyEntityMetadata, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			if err := attempt.AddRunStartFacts(ctx, runID, ref); err != nil || !attempt.effects.HasDeclarations() {
				t.Fatalf("canonical contribution failed: %v", err)
			}
			if reflect.ValueOf(attempt.effects).Pointer() != before || attempt.tx != tx {
				t.Fatal("start introduced a second effect or transaction owner")
			}
			if err := attempt.AddRunStartFacts(ctx, uuid.NewString(), ref); err == nil {
				t.Fatal("foreign run borrowed this creation")
			}
			impostor := *attempt
			if err := impostor.DeclareRunStart(ctx, runID, fact.BundleHash(), ""); err == nil {
				t.Fatal("same transaction admitted a different attempt")
			}
		})
	})
}

func TestRunStartInitialConstructionCapturesOnlyTheOwnedTree(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		runID, eventID, downstream := uuid.NewString(), uuid.NewString(), uuid.NewString()
		fact := runAdmissionFact(t, "d")
		withRunAdmissionAttempt(t, dialect, context.Background(), func(ctx context.Context, attempt *Attempt, _ *sql.Tx) {
			root := flowidentity.Instance{TemplateID: "root", ScopeKey: "root", InstanceID: runID,
				InstancePath: runID, EntityID: runID, HasStoredPath: true}
			if initial, err := attempt.BeginInitialRunConstruction(ctx, runID, root, eventID); err != nil || initial {
				t.Fatalf("ordinary construction invented run birth: %v %v", initial, err)
			}
			if err := attempt.DeclareRunStart(ctx, runID, fact.BundleHash(), eventID); err != nil {
				t.Fatal(err)
			}
			if initial, err := attempt.BeginInitialRunConstruction(ctx, runID, root, uuid.NewString()); err == nil || initial {
				t.Fatal("root borrowed another creating input")
			}
			if initial, err := attempt.BeginInitialRunConstruction(ctx, runID, root, eventID); err != nil || !initial {
				t.Fatalf("initial root not captured: %v %v", initial, err)
			}
			childEntity, childMutation, timer := uuid.NewString(), uuid.NewString(), uuid.NewString()
			for family, key := range map[privatefork.Family]string{
				privatefork.FamilyEntityMetadata:  childEntity,
				privatefork.FamilyEntityMutations: childMutation,
				privatefork.FamilyTimers:          timer,
			} {
				if err := attempt.AddFact(runID, family, key); err != nil {
					t.Fatal(err)
				}
			}
			if err := attempt.AddWholeFamily(runID, privatefork.FamilyEntityMetadata); err == nil {
				t.Fatal("whole run confused initial tree with later routed constructors")
			}
			if err := attempt.AddFact(runID, privatefork.FamilyEvents, eventID); err == nil {
				t.Fatal("initial tree captured its own creating ingress")
			}
			phase := BeforeAttempt
			if err := attempt.finalize(ctx, &phase); err == nil {
				t.Fatal("unfinished initial tree finalized")
			}
			if err := attempt.EndInitialRunProjection(ctx, runID); err != nil {
				t.Fatal(err)
			}
			if err := attempt.AddFact(runID, privatefork.FamilyEntityMetadata, childEntity); err == nil {
				t.Fatal("a post-construction write replaced the pre-ingress projection at the same revision")
			}
			if err := attempt.AddWholeFamily(runID, privatefork.FamilyEntityMetadata); err == nil {
				t.Fatal("whole-family capture replaced initial values at the same revision")
			}
			if err := attempt.AddFact(runID, privatefork.FamilyEntityMetadata, downstream); err != nil {
				t.Fatal(err)
			}
			projection, declared := attempt.effects.DeclaredStart(runID)
			if !declared || len(projection.Facts) != 3 {
				t.Fatalf("initial fact scope leaked: %+v", projection)
			}
			for _, ref := range projection.Facts {
				if ref.Key == downstream || ref.Key == eventID {
					t.Fatal("downstream construction or ingress entered exclusive start")
				}
			}
		})
	})
}

func TestRunStartPreservesCallerCancellationDuringSQLDrain(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		caller, cancel := context.WithCancel(context.Background())
		defer cancel()
		withRunAdmissionAttempt(t, dialect, caller, func(ctx context.Context, attempt *Attempt, _ *sql.Tx) {
			runID, fact := uuid.NewString(), runAdmissionFact(t, "b")
			canceled, cancelCurrent := context.WithCancel(ctx)
			cancelCurrent()
			if err := attempt.DeclareRunStart(canceled, runID, fact.BundleHash(), ""); !errors.Is(err, context.Canceled) {
				t.Fatalf("current cancellation discarded: %v", err)
			}
			cancel()
			if err := attempt.DeclareRunStart(context.WithoutCancel(ctx), runID, fact.BundleHash(), ""); !errors.Is(err, context.Canceled) {
				t.Fatalf("logical cancellation discarded by SQL drain: %v", err)
			}
			if attempt.effects.HasDeclarations() {
				t.Fatal("canceled creation leaked a start projection")
			}
		})
	})
}

func TestRunStartCannotPublishDuringCleanupOrParentDeletion(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		withRunAdmissionAttempt(t, dialect, context.Background(), func(ctx context.Context, attempt *Attempt, _ *sql.Tx) {
			runID, fact := uuid.NewString(), runAdmissionFact(t, "c")
			attempt.cleanup = true
			if err := attempt.DeclareRunStart(ctx, runID, fact.BundleHash(), ""); err == nil {
				t.Fatal("cleanup minted a creation cut")
			}
			attempt.cleanup = false
			attempt.kind = WholeParentDeletion
			if err := attempt.DeclareRunStart(ctx, runID, fact.BundleHash(), ""); err == nil {
				t.Fatal("destructive parent deletion minted a creation cut")
			}
			attempt.kind = Ordinary
			if attempt.effects.HasDeclarations() {
				t.Fatal("refused destructive contribution changed effects")
			}
		})
	})
}

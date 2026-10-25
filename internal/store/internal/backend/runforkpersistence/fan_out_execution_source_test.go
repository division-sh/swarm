package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type retainedSourceAssociationFixture struct {
	ctx                          context.Context
	db                           *sql.DB
	postgres                     *postgresbackend.Backend
	sqlite                       *sqlitebackend.Backend
	parent, target               sourceartifact.RootedSource
	parentID, childID, bindingID string
	lifecycle                    interface {
		ReviseRunSource(context.Context, runlifecycle.SourceRevisionRequest) (runlifecycle.MutationDisposition, error)
	}
}

// BindRetainedSourceAssociationForTest forwards only the native responsibility
// binding and its private fixed-cut proof. Composition and runs belong to the
// external fixture's original store and genuine process capability.
func BindRetainedSourceAssociationForTest(
	t *testing.T, ctx context.Context, db *sql.DB, postgres bool,
	parent, target sourceartifact.RootedSource, parentID, childID, operationID string,
	lifecycle interface {
		ReviseRunSource(context.Context, runlifecycle.SourceRevisionRequest) (runlifecycle.MutationDisposition, error)
	},
) (string, func(*testing.T, string)) {
	t.Helper()
	f := retainedSourceAssociationFixture{ctx: ctx, db: db, parent: parent, target: target, parentID: parentID, childID: childID, lifecycle: lifecycle}
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}
	selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: target.Artifact().BundleHash()}
	bind := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			binding, err := insertRunForkSelectedContractBinding(ctx, tx, runfork.RunForkSelectedContractBindingRequest{
				ForkRunID: childID, SourceRunID: parentID, ForkPoint: point, ContractSelection: selection, TargetRootAdmission: target.Admission(),
			}, time.Now())
			if err != nil {
				return err
			}
			f.bindingID = binding.BindingID
			_, _, err = bindForkOperationTx(ctx, tx, runfork.ForkOperationRequest{
				OperationID: operationID, Actor: "bearer:rooted-source-proof", TransportHash: "sha256:rooted-source-proof",
				SourceRunID: parentID, TargetBundleHash: target.Artifact().BundleHash(), TargetRootReference: target.Admission().Reference(),
				ContractSelection: selection, ResolvedPoint: &point,
			}, childID, binding.BindingID, postgres)
			return err
		})
		return struct{}{}, err
	}
	var err error
	if postgres {
		f.postgres, err = postgresbackend.New(db)
		if err == nil {
			err = mutationprotocol.RunPostgres(ctx, f.postgres, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, bind).Err()
		}
	} else {
		f.sqlite, err = sqlitebackend.New(db)
		if err == nil {
			err = mutationprotocol.RunSQLite(ctx, f.sqlite, "retained source responsibility fixture", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, bind).Err()
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return f.bindingID, func(t *testing.T, child string) { proveRetainedFanOutExecutionRoot(t, f, child) }
}

func (f retainedSourceAssociationFixture) reviseChildRoot(t *testing.T, source sourceartifact.RootedSource) {
	t.Helper()
	if _, err := f.lifecycle.ReviseRunSource(f.ctx, runlifecycle.SourceRevisionRequest{
		RunID: f.childID, Source: sourceartifactfixture.FactFor(source.Artifact()), RootAdmission: source.Admission(),
	}); err != nil {
		t.Fatal(err)
	}
}

func (f retainedSourceAssociationFixture) changeEvidence(t *testing.T, statement string, args ...any) {
	t.Helper()
	result, err := f.db.ExecContext(context.Background(), statement, args...)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("corrupt/restore exact retained evidence: affected=%d err=%v", count, err)
	}
}

func proveRetainedFanOutExecutionRoot(t *testing.T, f retainedSourceAssociationFixture, child string) {
	t.Helper()
	ctx := f.ctx
	_, capsule := fanOutOwnershipFixture(t, ".", "entityless")
	capsule.Receiver = nil
	capsule.Entity, capsule.PlatformEntity, capsule.Computed, capsule.Accumulated = nil, nil, nil, nil
	capsule.Join, capsule.StateFields, capsule.StateBookkeeping = nil, nil, nil
	capsule.Lineage.RunID, capsule.Lineage.ParentEventID = f.parentID, uuid.NewString()
	digest, err := canonicaljson.Hash(capsule.SourceProjection)
	if err != nil {
		t.Fatal(err)
	}
	capsule.SourcePlanRef = contracts.FanOutPlanRef{BundleHash: f.parent.Artifact().BundleHash(), ElementRef: capsule.SourceProjection.ElementRef, SemanticDigest: digest}
	raw, err := fanoutobligation.MarshalCapsule(capsule)
	if err != nil {
		t.Fatal(err)
	}
	at, delivery, ordinal := time.Now().UTC(), uuid.NewString(), 0
	intent := runForkRevisionFanOutFact{FactKind: "intent", OriginKind: "handler", TriggeringDeliveryID: delivery,
		FlowPath: ".", DeclarationFamily: capsule.SourcePlanRef.ElementRef.Family, SemanticPath: capsule.SourcePlanRef.ElementRef.SemanticPath,
		BundleHash: capsule.SourcePlanRef.BundleHash, SemanticDigest: digest, SourceKind: string(fanoutobligation.SourceEventPayloadField),
		SourceEventID: capsule.Lineage.ParentEventID, SourceField: "items", Cardinality: 1, Status: "open", Capsule: raw, CreatedAt: at}
	a := &runForkRevisionSnapshot{RunID: f.parentID, Revision: 7, FanOutFacts: []runForkRevisionFanOutFact{intent}}
	a.Events = []runForkRevisionEvent{{RunID: f.parentID, EventID: capsule.Lineage.ParentEventID, EventName: capsule.HandlerEventKey, CreatedAt: at}}
	a.Deliveries = []runForkRevisionDelivery{{Snapshot: deliverylifecycle.HistoricalFact{RunID: f.parentID, DeliveryID: delivery, EventID: capsule.Lineage.ParentEventID, Status: deliverylifecycle.StatusDelivered}}}
	intent.Status, intent.Cursor = "closed", 1
	outcome := runForkRevisionFanOutFact{FactKind: "outcome", OriginKind: "handler", TriggeringDeliveryID: delivery,
		FlowPath: intent.FlowPath, DeclarationFamily: intent.DeclarationFamily, SemanticPath: intent.SemanticPath,
		Ordinal: &ordinal, OutcomeKind: "committed", SourceOutcomeEventID: uuid.NewString(), InheritedDisposition: "no_route", CreatedAt: at}
	b := &runForkRevisionSnapshot{RunID: f.childID, Revision: 9, FanOutFacts: []runForkRevisionFanOutFact{intent, outcome}}
	c := &runForkRevisionSnapshot{RunID: child, Revision: 10, FanOutFacts: []runForkRevisionFanOutFact{intent, outcome}}
	projected, err := loadRunForkFanOutObligationsFromRevision(c, nil)
	if err != nil || len(projected) != 1 {
		t.Fatalf("closed history projection: %v", err)
	}
	// Cuts are supplied as already-resolved immutable projections here;
	// origins and exact root/content restoration use the native stores.
	load := func(ctx context.Context, obligation runfork.RunForkFanOutObligation) (sourceartifact.RootedSource, error) {
		var rooted sourceartifact.RootedSource
		read := func(ctx context.Context, tx *sql.Tx) (err error) {
			resolver := capturedConstructionResolver{tx: tx, snapshots: map[constructionCut]*runForkRevisionSnapshot{
				{runID: a.RunID, revision: a.Revision}: a, {runID: b.RunID, revision: b.Revision}: b,
			}, active: map[string]bool{}}
			rooted, err = fanOutExecutionSourceAtCut(ctx, &resolver, c, obligation, f.postgres != nil, map[string]bool{})
			return err
		}
		var err error
		if f.postgres != nil {
			err = f.postgres.RunReadTransaction(ctx, read)
		} else {
			err = f.sqlite.RunReadTransaction(ctx, read)
		}
		if err != nil {
			return sourceartifact.RootedSource{}, err
		}
		return rooted, nil
	}
	rooted, err := load(ctx, projected[0])
	if err != nil || !rooted.Admission().Equal(f.target.Admission()) || rooted.Admission().Equal(f.parent.Admission()) {
		t.Fatalf("A trigger/C key selected a root other than B execution: %+v, %v", rooted.Admission().Reference(), err)
	}
	f.reviseChildRoot(t, f.parent)
	if loaded, err := load(ctx, projected[0]); err == nil || !strings.Contains(err.Error(), "differs from its exact binding") || loaded.Artifact() != nil {
		t.Fatal("reassociated B header replaced the retained operation root")
	}
	f.reviseChildRoot(t, f.target)
	f.changeEvidence(t, `UPDATE run_fork_selected_contract_bindings SET fork_run_id=$1 WHERE binding_id=$2`, f.parentID, f.bindingID)
	if loaded, err := load(ctx, projected[0]); err == nil || !strings.Contains(err.Error(), "ordinary fan-out fork changed root") || loaded.Artifact() != nil {
		t.Fatal("missing selected binding became ordinary same-content root authority")
	}
	f.changeEvidence(t, `UPDATE run_fork_selected_contract_bindings SET fork_run_id=$1 WHERE binding_id=$2`, f.childID, f.bindingID)
	for _, variant := range []string{"missing-obligation", "foreign-key", "changed-reference", "relabeled-native"} {
		t.Run(variant, func(t *testing.T) {
			bad := projected[0]
			facts := b.FanOutFacts
			want := "no exact obligation at its immutable source cut"
			switch variant {
			case "missing-obligation":
				b.FanOutFacts = nil
			case "foreign-key":
				bad.Intent.Request.Key.TriggeringDeliveryID = uuid.NewString()
			case "changed-reference":
				bad.Intent.Request.PlanRef.BundleHash = "bundle-v2:sha256:" + strings.Repeat("f", 64)
				want = "inert fan-out history changed its retained execution reference"
			case "relabeled-native":
				bad.Intent.Request.Capsule.Lineage.RunID = child
				want = "lacks its exact triggering delivery"
			}
			loaded, err := load(ctx, bad)
			b.FanOutFacts = facts
			if err == nil || !strings.Contains(err.Error(), want) || loaded.Artifact() != nil {
				t.Fatalf("contradictory history disclosed an execution source: %v", err)
			}
		})
	}
	f.changeEvidence(t, `UPDATE source_root_admissions SET root_directory=$1 WHERE root_directory=$2`, "retired-execution", f.target.Admission().RootDirectory())
	if loaded, err := load(ctx, projected[0]); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) || loaded.Artifact() != nil {
		t.Fatal("missing B root was replaced by same-content A/C root")
	}
	f.changeEvidence(t, `UPDATE source_root_admissions SET root_directory=$1 WHERE root_directory=$2`, f.target.Admission().RootDirectory(), "retired-execution")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := load(canceled, projected[0]); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled history read: %v", err)
	}
}

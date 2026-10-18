package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

// ClosedSelectedCopiedJournalHistory proves cohort membership, not a producing
// generation or permission to execute. The original NULL binding stays NULL.
type ClosedSelectedCopiedJournalHistory struct {
	Record       pipeline.ActivityAttemptRecord
	Origin       semanticview.ActivityRequestOrigin
	InputEventID string
}

func readClosedSelectedCopiedJournalTx(ctx context.Context, tx *sql.Tx, permanent ClosedSelectedPermanentForkCohort, record pipeline.ActivityAttemptRecord, sqlite bool, original semanticview.OriginalLoopCarriage) (ClosedSelectedCopiedJournalHistory, error) {
	var absent ClosedSelectedCopiedJournalHistory
	validate, resolve := runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint
	loadSource := sourceadmission.LoadPostgresTx
	dialect := activityjournal.DialectPostgres
	if sqlite {
		validate, resolve = runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint
		loadSource, dialect = sourceadmission.LoadSQLiteTx, activityjournal.DialectSQLite
	}
	admission, err := loadRunForkSourceStateAdmission(ctx, tx, permanent.Binding.ForkRunID, validate, resolve, !sqlite)
	if err != nil {
		return absent, err
	}
	admission.carriage = original
	admission.sourceAt = func(ctx context.Context, tx *sql.Tx, runID string) (correlation.SourceArtifactFact, error) {
		return loadSource(ctx, tx, runID, false, true)
	}
	cut, err := newRunForkReplayCut(admission.snapshot)
	if err != nil {
		return absent, err
	}
	physical, err := readCopiedSelectedInputTx(ctx, tx, permanent.Binding.ForkRunID, record.SourceEventID, sqlite)
	if err != nil {
		return absent, err
	}
	lineage, _ := physical.SelectedForkLineage()
	parent, err := cut.event(lineage.SourceEventID())
	if err != nil {
		return absent, err
	}
	origin, err := admission.activityRequestOriginAtCut(ctx, tx, parent.ID(), make(map[string]bool))
	if err != nil {
		return absent, err
	}
	source := runfork.RunForkSelectedContractSourceEvent{SourceEventID: parent.ID(), EventName: string(parent.Type()),
		ExecutionMode: parent.ExecutionMode(), RoutingSource: parent.RoutingSource(), Payload: parent.Payload()}
	projected, state, err := admission.project(source)
	if err != nil {
		return absent, err
	}
	var request runForkActivityRequestPayload
	if err := json.Unmarshal(parent.Payload(), &request); err != nil {
		return absent, err
	}
	if err := validateCopiedActivityDeclaration(original, origin, request); err != nil {
		return absent, err
	}
	fact, err := runForkActivityFact(request)
	if err != nil {
		return absent, err
	}
	requestID, _, err := admitRunForkActivitySourceIdentity(ctx, tx, admission, source, fact)
	if err != nil {
		return absent, err
	}
	evidence, err := loadRunForkActivityAttemptEvidence(ctx, tx, requestID)
	if err != nil {
		return absent, err
	}
	if err := validateSourceActivityEvidence(evidence, request, state, requestID); err != nil {
		return absent, err
	}
	proposed, err := admission.requireOriginalActivityApproval(ctx, tx, origin)
	if err != nil {
		return absent, err
	}
	if proposed && evidence.Status == pipeline.ActivityAttemptStatusUncertain {
		return absent, errors.New("copied proposed effect has ambiguous dispatch evidence")
	}
	reference, err := admission.activityPublicationGenerationProjection(parent, request.Generation)
	if err != nil {
		return absent, err
	}
	projected.Payload, err = bindRunForkActivitySourceEvent(projected.Payload, permanent.Binding.ForkRunID, parent.ID(), state.Fork.EntityID, reference)
	if err != nil {
		return absent, err
	}
	if err := json.Unmarshal(projected.Payload, &request); err != nil {
		return absent, err
	}
	fact, err = runForkActivityFact(request)
	if err != nil {
		return absent, err
	}
	if record.RequestEventID != activityidentity.RequestEventID(fact) || !record.ExecutionBinding.IsZero() {
		return absent, errors.New("copied journal has no exact NULL-bound child request identity")
	}
	if err := admission.confirmCopiedJournalInputTx(ctx, tx, permanent, physical, parent, reference, projected); err != nil {
		return absent, err
	}
	replyID, err := admission.projectReplyReference(evidence.ReplyContextID)
	if err != nil {
		return absent, err
	}
	if err := confirmCopiedActivityEvidenceTx(ctx, tx, permanent.Binding.ForkRunID, state.Fork.FlowInstance, request, reference, evidence, replyID); err != nil {
		return absent, err
	}
	actual, present, err := activityjournal.Load(ctx, tx, dialect, record.RequestEventID)
	if err != nil || !present || !reflect.DeepEqual(actual, record) {
		return absent, errors.Join(errors.New("copied journal changed within its fenced capture"), err)
	}
	return ClosedSelectedCopiedJournalHistory{Record: record, Origin: origin, InputEventID: physical.ID()}, nil
}

func readCopiedSelectedInputTx(ctx context.Context, tx *sql.Tx, runID, eventID string, sqlite bool) (events.Event, error) {
	var absent events.Event
	var physical events.AdmittedEvent
	var found bool
	var err error
	if sqlite {
		physical, _, found, err = eventrecordsqlite.LoadAdmitted(ctx, tx, eventID)
	} else {
		physical, _, found, err = eventrecordpostgres.LoadAdmitted(ctx, tx, eventID)
	}
	if err != nil || !found {
		return absent, errors.Join(fmt.Errorf("copied journal lacks committed child input %s in run %s", eventID, runID), err)
	}
	if _, selected := physical.Event().SelectedForkLineage(); !selected || physical.Event().RunID() != runID {
		return absent, errors.New("copied journal input lacks its own child's selected lineage")
	}
	return physical.Event(), nil
}

func (a runForkSourceStateAdmission) confirmCopiedJournalInputTx(ctx context.Context, tx *sql.Tx, permanent ClosedSelectedPermanentForkCohort, physical, parent events.Event, reference *loopruntime.ForkChildReference, projected runfork.RunForkSelectedContractSourceEvent) error {
	if _, err := requireSelectedPublicationCutFrame(ctx, tx, permanent.Binding, physical, parent, reference); err != nil {
		return err
	}
	if physical.RoutingSource() != projected.RoutingSource || !workflowCommitJSONEqual(physical.Payload(), projected.Payload) {
		return errors.New("copied journal child input differs from its fixed-cut projection")
	}
	childSource, err := a.sourceAt(ctx, tx, permanent.Binding.ForkRunID)
	if err != nil {
		return err
	}
	if childSource.BundleHash() != permanent.Operation.Request.TargetBundleHash {
		return errors.New("copied journal child source differs from its permanent contract")
	}
	return requireOriginalActivityRequestArtifact(physical, childSource)
}

func validateCopiedActivityDeclaration(original semanticview.OriginalLoopCarriage, origin semanticview.ActivityRequestOrigin, request runForkActivityRequestPayload) error {
	if err := original.RequireSource(origin.Cuts[0].Source.BundleHash()); err != nil {
		return err
	}
	owner, err := activityidentity.ParseOwnerKey(request.NodeID)
	if err != nil {
		return err
	}
	node, isNode := owner.Node()
	if !isNode {
		return errors.New("copied journal requires its original declared activity node")
	}
	role, declared, err := original.ResolveOriginalActivity(origin, node, request.HandlerEventKey, request.ActivityID, request.Tool)
	if err != nil {
		return err
	}
	if request.Generation != (attemptgeneration.Generation{}) {
		if !declared || request.Generation.FlowID != role.FlowID() || request.Generation.LoopID != role.LoopID() || request.Generation.RevisionField != role.RevisionField() {
			return errors.New("copied journal generation differs from its original declaration")
		}
	} else if declared {
		return errors.New("copied loop activity journal lacks its declared generation")
	}
	return nil
}

package runforkpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

// A selected publication and its journal have distinct identities. Historical
// publication evidence identifies the former; the activity owner identifies the
// latter. Neither grants this new fork permission to execute.
func admitRunForkActivitySourceIdentity(ctx context.Context, tx *sql.Tx, admission runForkSourceStateAdmission, source runfork.RunForkSelectedContractSourceEvent, fact activityidentity.Fact) (requestID string, continued bool, err error) {
	if tx == nil || admission.snapshot == nil {
		return "", false, fmt.Errorf("activity source identity requires its native fixed-cut admission")
	}
	requestID = activityidentity.RequestEventID(fact)
	if requestID == source.SourceEventID {
		return requestID, false, nil
	}
	var admitted events.AdmittedEvent
	var found bool
	if admission.postgres {
		admitted, _, found, err = eventrecordpostgres.LoadAdmitted(ctx, tx, source.SourceEventID)
	} else {
		admitted, _, found, err = eventrecordsqlite.LoadAdmitted(ctx, tx, source.SourceEventID)
	}
	if err != nil || !found {
		return "", false, fmt.Errorf("continued activity requires its exact admitted publication: %v", err)
	}
	original := admitted.Event()
	if original.ID() != source.SourceEventID || original.RunID() != admission.snapshot.RunID || string(original.Type()) != source.EventName ||
		original.ExecutionMode() != source.ExecutionMode || original.RoutingSource() != source.RoutingSource || !bytes.Equal(original.Payload(), source.Payload) {
		return "", false, fmt.Errorf("activity source publication contradicts its fixed-cut input")
	}
	selected, selectedPublication := original.SelectedForkLineage()
	if !selectedPublication {
		return "", false, fmt.Errorf("activity request identity contradicts its exact source coordinates")
	}
	if _, err := pipeline.SelectedActivityRequestLineage(original); err != nil {
		return "", false, err
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, original.RunID())
	if err != nil {
		return "", false, err
	}
	lineage, found, err := readSelectedForkInputLineageTx(ctx, tx, original.RunID(), selected.SourceEventID(), original.ID())
	if err != nil || !found {
		return "", false, fmt.Errorf("continued activity lacks committed selected publication lineage: %v", err)
	}
	parent := runfork.RunForkSelectedContractSourceEvent{SourceEventID: selected.SourceEventID(), EventName: source.EventName, ExecutionMode: source.ExecutionMode}
	if err := validateSelectedForkPublishedInputIdentity(binding, lineage, original, parent); err != nil {
		return "", false, err
	}
	if err := validateSelectedForkPublishedInputFrame(binding, lineage, original, parent); err != nil {
		return "", false, err
	}
	return requestID, true, nil
}

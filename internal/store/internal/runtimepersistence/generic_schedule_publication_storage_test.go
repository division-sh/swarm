package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestGenericSchedulePublicationObservationsKeepExactProducerAndOriginalCoordinatorBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store.(diagnosticRuntimeLogFixtureStore), testAuthorActivityContext()
			root := runForkSnapshotFixtureEvent(t, ctx, selected)
			taskID := "exact-task"
			wrongProducer := eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", taskID, []byte(`{"log_level":"info","message":"unrelated platform publication"}`), 0, root.RunID(), "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitDiagnosticRuntimeLogFixture(ctx, selected, wrongProducer); err != nil {
				t.Fatal(err)
			}
			wrongType := eventtest.ExistingRunRootIngress(uuid.NewString(), "snapshot.fixture", genericschedule.OccurrenceProducerID(), taskID, []byte(`{"value":2}`), 0, root.RunID(), events.EventEnvelope{}, time.Now().UTC())
			wrongType, err := bindSemanticEventFixturePayload(wrongType)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := events.AdmitForPersistence(wrongType, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			if inserted, err := CommitSemanticEventFixtureForTest(ctx, selected, admitted, testRouteSettlement(wrongType, nil), nil, pipelineobligation.ScopeDirect, nil); err != nil || !inserted {
				t.Fatalf("commit wrong-type control: %t/%v", inserted, err)
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			modes, modesErr := ReadGenericSchedulePublicationModesForTest(ctx, selected, taskID)
			distinct, distinctErr := ReadGenericScheduleDistinctPublicationModesForTest(ctx, selected, taskID)
			count, countErr := CountGenericSchedulePublicationsForTest(ctx, selected, taskID)
			if modesErr != nil || distinctErr != nil || countErr != nil || modes != (GenericSchedulePublicationModes{}) || distinct != (GenericSchedulePublicationModes{}) || count != 0 {
				t.Fatalf("unrelated publication acquired schedule evidence: modes=%+v/%v distinct=%+v/%v count=%d/%v", modes, modesErr, distinct, distinctErr, count, countErr)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("schedule observation escaped original coordinator: %+v", counts)
			}
		})
	}
}

func TestGenericSchedulePublicationObservationsRefuseRawInvalidAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, selected any, taskID string) {
		t.Helper()
		modes, modesErr := ReadGenericSchedulePublicationModesForTest(ctx, selected, taskID)
		distinct, distinctErr := ReadGenericScheduleDistinctPublicationModesForTest(ctx, selected, taskID)
		count, countErr := CountGenericSchedulePublicationsForTest(ctx, selected, taskID)
		if modesErr == nil || distinctErr == nil || countErr == nil || modes != (GenericSchedulePublicationModes{}) || distinct != (GenericSchedulePublicationModes{}) || count != 0 {
			t.Fatalf("refused owner retained schedule evidence: %+v/%v %+v/%v %d/%v", modes, modesErr, distinct, distinctErr, count, countErr)
		}
	}
	for _, selected := range []any{nil, &sql.DB{}, &sql.Tx{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		refuse(context.Background(), selected, "task")
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			for _, invalid := range []string{"", " \t"} {
				refuse(ctx, selected, invalid)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			refuse(cancelled, selected, "task")
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(ctx, selected, "task")
		})
	}
}

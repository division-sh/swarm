package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestPublicationStorageObservationsUseOriginalReadOwnerBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			header, err := ReadExistingPublicationRunStorageForTest(ctx, fixture.store, event.RunID())
			if err != nil || header.Status != "running" || header.TriggerEventType != "" || header.TriggerEventID != "" || header.BundleHash != authorActivityTestBundleHash {
				t.Fatalf("existing header NULL defaults/source changed: %+v %v", header, err)
			}
			trigger, err := ReadPublicationRunTriggerStorageForTest(ctx, fixture.store, event.RunID())
			if err != nil || trigger != (PublicationRunTriggerStorage{Status: "running"}) {
				t.Fatalf("three-column trigger projection changed: %+v %v", trigger, err)
			}
			projection, err := ReadPublicationEventProjectionForTest(ctx, fixture.store, event.ID())
			var payload map[string]int
			if err != nil || json.Unmarshal(projection.Payload, &payload) != nil || payload["value"] != 1 || projection.EntityID != "" || projection.FlowInstance != "" || projection.ProducedBy != "fixture" || projection.TargetRoute != "{}" || projection.TargetSet != "[]" {
				t.Fatalf("physical event projection/defaults changed: %+v %v", projection, err)
			}
			identity, err := ReadPublicationEventIdentityStorageForTest(ctx, fixture.store, event.ID())
			if err != nil || identity != (PublicationEventIdentityStorage{RunID: event.RunID(), EventName: "snapshot.fixture", ProducedBy: "fixture"}) {
				t.Fatalf("event identity scope changed: %+v %v", identity, err)
			}
			reference, err := ReadOperatorReferenceStorageForTest(ctx, fixture.store, event.ID())
			if err != nil || reference != (OperatorReferenceStorage{}) {
				t.Fatalf("reference NULL defaults changed: %+v %v", reference, err)
			}
			targets, err := ReadPublicationDeliveryTargetEncodingsForTest(ctx, fixture.store, event.ID())
			if err != nil || len(targets) != 0 {
				t.Fatalf("empty delivery observation changed: %+v %v", targets, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 6 || counts.Total.ReadCommits != 6 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("observations escaped original selected read owner: %+v", counts)
			}
			if got, err := ReadCreatedPublicationRunStorageForTest(ctx, fixture.store, event.RunID()); err == nil || got != (PublicationRunStorage{}) {
				t.Fatalf("strict creation trigger silently coalesced NULL or returned a partial header: %+v %v", got, err)
			}
			if backend.name == "postgres" {
				got, err := ReadPostgresRootTemplatePublicationForTest(ctx, fixture.store, event.ID())
				if err != nil || got.EventName != "snapshot.fixture" || got.EntityID != "" || got.FlowInstance != "" || string(got.Payload) != string(projection.Payload) {
					t.Fatalf("root/template projection added inference or changed bytes: %+v %v", got, err)
				}
				if got, err := ReadPostgresRunStartStorageForTest(ctx, fixture.store, event.RunID(), "snapshot.fixture"); err == nil || !reflect.DeepEqual(got, RunStartStorage{}) {
					t.Fatalf("strict run.start header silently repaired NULL or exposed partial evidence: %+v %v", got, err)
				}
			}
		})
	}
}

type publicationReadCase struct {
	name string
	read func(context.Context, any) (any, error)
	zero any
}

func publicationReadCases(runID, eventID string) []publicationReadCase {
	return []publicationReadCase{
		{"created-run", func(ctx context.Context, owner any) (any, error) {
			return ReadCreatedPublicationRunStorageForTest(ctx, owner, runID)
		}, PublicationRunStorage{}},
		{"existing-run", func(ctx context.Context, owner any) (any, error) {
			return ReadExistingPublicationRunStorageForTest(ctx, owner, runID)
		}, PublicationRunStorage{}},
		{"trigger", func(ctx context.Context, owner any) (any, error) {
			return ReadPublicationRunTriggerStorageForTest(ctx, owner, runID)
		}, PublicationRunTriggerStorage{}},
		{"event", func(ctx context.Context, owner any) (any, error) {
			return ReadPublicationEventProjectionForTest(ctx, owner, eventID)
		}, PublicationEventProjection{}},
		{"identity", func(ctx context.Context, owner any) (any, error) {
			return ReadPublicationEventIdentityStorageForTest(ctx, owner, eventID)
		}, PublicationEventIdentityStorage{}},
		{"reference", func(ctx context.Context, owner any) (any, error) {
			return ReadOperatorReferenceStorageForTest(ctx, owner, eventID)
		}, OperatorReferenceStorage{}},
		{"targets", func(ctx context.Context, owner any) (any, error) {
			return ReadPublicationDeliveryTargetEncodingsForTest(ctx, owner, eventID)
		}, []string(nil)},
		{"stored-response", func(ctx context.Context, owner any) (any, error) {
			return ReadEventPublicationStoredResponseForTest(ctx, owner, "absent")
		}, []byte(nil)},
	}
}

func TestPublicationStorageObservationsRefusePartialInvalidCancelledAndClosedBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, fixture.store)
			cases := publicationReadCases(event.RunID(), event.ID())
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			for _, item := range cases {
				if got, err := item.read(cancelled, fixture.store); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, item.zero) {
					t.Fatalf("%s cancellation exposed provisional evidence: %+v %v", item.name, got, err)
				}
				if got, err := item.read(ctx, struct{}{}); err == nil || !reflect.DeepEqual(got, item.zero) {
					t.Fatalf("%s accepted foreign ownership: %+v %v", item.name, got, err)
				}
			}
			if got, err := ReadPublicationEventProjectionForTest(ctx, fixture.store, "invalid"); err == nil || !reflect.DeepEqual(got, PublicationEventProjection{}) {
				t.Fatalf("invalid event identity yielded evidence: %+v %v", got, err)
			}
			if got, err := ReadPublicationEventProjectionForTest(ctx, fixture.store, uuid.NewString()); err == nil || !reflect.DeepEqual(got, PublicationEventProjection{}) {
				t.Fatalf("absent event yielded evidence: %+v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, item := range cases {
				if got, err := item.read(ctx, fixture.store); err == nil || !reflect.DeepEqual(got, item.zero) {
					t.Fatalf("%s closed owner yielded evidence: %+v %v", item.name, got, err)
				}
			}
		})
	}
}

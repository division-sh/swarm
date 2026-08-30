package runtimepersistence

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestRunAPIReadSurface_LoadAndListRunHeaders(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	now := time.Unix(1700000000, 0).UTC()
	newer := uuid.NewString()
	middle := uuid.NewString()
	older := uuid.NewString()
	newerEvent := uuid.NewString()
	middleEvent := uuid.NewString()
	olderEvent := uuid.NewString()
	newerEntityA := uuid.NewString()
	newerEntityB := uuid.NewString()
	middleEntity := uuid.NewString()
	olderEventOnlyA := uuid.NewString()
	olderEventOnlyB := uuid.NewString()
	bundleA := "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bundleB := "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	failedRunFailure := testFailureEnvelope(runtimefailures.ClassInternalFailure, "run_failed", nil)
	for _, snapshot := range []runlifecyclefixture.CorruptSnapshot{
		{
			RunID: newer, State: "running", BundleHash: bundleA,
			OriginKind:     string(runtimerunlifecycle.OriginEvent),
			TriggerEventID: newerEvent, TriggerEventType: "scan.requested",
			EntityCount: 3, EventCount: 2, StartedAt: now,
		},
		{
			RunID: middle, State: "completed", BundleHash: bundleB,
			OriginKind:      string(runtimerunlifecycle.OriginForkMaterialization),
			ForkedFromRunID: newer, ForkedFromEventID: newerEvent,
			EntityCount: 5, EventCount: 1,
			StartedAt: now.Add(-time.Hour), EndedAt: now.Add(-30 * time.Minute),
		},
		{
			RunID: older, State: "failed", BundleHash: bundleA,
			OriginKind:     string(runtimerunlifecycle.OriginEvent),
			TriggerEventID: olderEvent, TriggerEventType: "scan.failed",
			EntityCount: 1, EventCount: 1, Failure: &failedRunFailure,
			StartedAt: now.Add(-2 * time.Hour), EndedAt: now.Add(-90 * time.Minute),
		},
	} {
		runlifecyclefixture.RequireCorruptPostgresSnapshot(t, ctx, db, snapshot)
	}
	for _, fixture := range []struct {
		id        string
		runID     string
		eventType events.EventType
		entityID  string
		createdAt time.Time
	}{
		{id: newerEvent, runID: newer, eventType: "scan.requested", createdAt: now.Add(time.Second)},
		{id: uuid.NewString(), runID: newer, eventType: "scan.completed", createdAt: now.Add(2 * time.Second)},
		{id: middleEvent, runID: middle, eventType: "scan.requested", createdAt: now.Add(-time.Hour + time.Second)},
		{id: olderEvent, runID: older, eventType: "scan.failed", entityID: olderEventOnlyA, createdAt: now.Add(-2*time.Hour + time.Second)},
		{id: uuid.NewString(), runID: older, eventType: "scan.replayed", entityID: olderEventOnlyB, createdAt: now.Add(-2*time.Hour + 2*time.Second)},
	} {
		seedPostgresSemanticEventRecordFixture(
			t, ctx, db, fixture.id, fixture.runID, fixture.eventType,
			events.EventProducerAgent, "test", fixture.entityID, "", fixture.createdAt,
		)
	}
	seedPostgresEntityStateRows(t, db, ctx, newer, newerEntityA, newerEntityB)
	seedPostgresEntityStateRows(t, db, ctx, middle, middleEntity)

	header, err := pg.LoadRunHeader(ctx, middle)
	if err != nil {
		t.Fatalf("LoadRunHeader: %v", err)
	}
	if header.RunID != middle || header.Status != "completed" ||
		header.Origin.Kind() != runtimerunlifecycle.OriginForkMaterialization ||
		header.Origin.SourceRunID() != newer ||
		header.Origin.SourceEventID() != newerEvent {
		t.Fatalf("header = %#v", header)
	}
	if header.EndedAt == nil {
		t.Fatalf("header.EndedAt = nil, want terminal timestamp")
	}
	if header.EntityCount != 1 {
		t.Fatalf("header.EntityCount = %d, want entity_state count 1 despite stale run counter", header.EntityCount)
	}

	firstPage, cursor, err := pg.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Status: "running", Limit: 1})
	if err != nil {
		t.Fatalf("ListRunHeaders first page: %v", err)
	}
	if len(firstPage) != 1 || firstPage[0].RunID != newer {
		t.Fatalf("first page = %#v, want newer running run", firstPage)
	}
	if firstPage[0].EntityCount != 2 {
		t.Fatalf("first page entity_count = %d, want entity_state count 2 despite event undercount", firstPage[0].EntityCount)
	}
	if cursor != "" {
		t.Fatalf("running-only cursor = %q, want empty", cursor)
	}

	allFirstPage, cursor, err := pg.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("ListRunHeaders all first page: %v", err)
	}
	if len(allFirstPage) != 2 || allFirstPage[0].RunID != newer || allFirstPage[1].RunID != middle {
		t.Fatalf("all first page = %#v", allFirstPage)
	}
	if cursor == "" {
		t.Fatal("cursor empty for truncated run list")
	}
	allSecondPage, next, err := pg.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Limit: 2, Cursor: cursor})
	if err != nil {
		t.Fatalf("ListRunHeaders all second page: %v", err)
	}
	if len(allSecondPage) != 1 || allSecondPage[0].RunID != older || next != "" {
		t.Fatalf("all second page = %#v cursor=%q, want older only and no next cursor", allSecondPage, next)
	}
	if allSecondPage[0].EntityCount != 0 {
		t.Fatalf("older entity_count = %d, want entity_state count 0 despite event overcount", allSecondPage[0].EntityCount)
	}

	since := now.Add(-90 * time.Minute)
	recent, _, err := pg.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Since: &since})
	if err != nil {
		t.Fatalf("ListRunHeaders since: %v", err)
	}
	if len(recent) != 2 {
		t.Fatalf("recent runs len = %d, want 2: %#v", len(recent), recent)
	}
	bundleRuns, _, err := pg.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{BundleHash: bundleA, Limit: 10})
	if err != nil {
		t.Fatalf("ListRunHeaders bundle_hash: %v", err)
	}
	if len(bundleRuns) != 2 || bundleRuns[0].RunID != newer || bundleRuns[1].RunID != older {
		t.Fatalf("bundle-filtered runs = %#v, want newer and older only", bundleRuns)
	}
	runningBundleRuns, _, err := pg.ListRunHeaders(ctx, operatorread.RunHeaderListOptions{Status: "running", BundleHash: bundleA, Limit: 10})
	if err != nil {
		t.Fatalf("ListRunHeaders status+bundle_hash: %v", err)
	}
	if len(runningBundleRuns) != 1 || runningBundleRuns[0].RunID != newer {
		t.Fatalf("status+bundle-filtered runs = %#v, want newer only", runningBundleRuns)
	}
}

func TestRunAPIReadSurface_LoadRunHeaderNotFound(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	_, err := pg.LoadRunHeader(testAuthorActivityContext(), uuid.NewString())
	if !errors.Is(err, operatorread.ErrRunNotFound) {
		t.Fatalf("LoadRunHeader error = %v, want operatorread.ErrRunNotFound", err)
	}
}

package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestGlobalEventChronologyPreservesUnfilteredOrderFieldsAndOriginalReadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			at := time.Date(2026, 10, 8, 15, 0, 0, 0, time.UTC)
			for _, item := range []struct {
				id, run, name string
				offset        time.Duration
			}{
				{"00000000-0000-4000-8000-000000000003", "00000000-0000-4000-8000-000000000013", "chronology.later", time.Second},
				{"00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000012", "chronology.second", 0},
				{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000011", "chronology.first", 0},
			} {
				source := eventtest.StaticFlowRoutingSource("chronology", "chronology/"+item.run, item.run)
				envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, source.Route())
				event := eventtest.RunCreatingRootIngressWithRoutingSource(item.id, events.EventType(item.name), "runtime", "", []byte(`{}`), 0, item.run, "", envelope, source, at.Add(item.offset))
				if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			out, err := ReadGlobalEventChronologyForTest(ctx, fixture.store)
			want := []GlobalEventChronologyRow{
				{Name: "chronology.first", EntityID: "00000000-0000-4000-8000-000000000011", FlowInstance: "chronology/00000000-0000-4000-8000-000000000011"},
				{Name: "chronology.second", EntityID: "00000000-0000-4000-8000-000000000012", FlowInstance: "chronology/00000000-0000-4000-8000-000000000012"},
				{Name: "chronology.later", EntityID: "00000000-0000-4000-8000-000000000013", FlowInstance: "chronology/00000000-0000-4000-8000-000000000013"},
			}
			if err != nil || !reflect.DeepEqual(out, want) {
				t.Fatalf("global chronology=%+v,err=%v,want%+v", out, err, want)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("chronology escaped original read: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if out, err := ReadGlobalEventChronologyForTest(cancelled, fixture.store); !errors.Is(err, context.Canceled) || out != nil {
				t.Fatalf("cancelled chronology retained evidence: %+v,%v", out, err)
			}
			switch owner := fixture.store.(type) {
			case *PostgresStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if out, err := ReadGlobalEventChronologyForTest(ctx, fixture.store); err == nil || out != nil {
				t.Fatalf("closed chronology retained evidence: %+v,%v", out, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if out, err := ReadGlobalEventChronologyForTest(context.Background(), invalid); err == nil || out != nil {
			t.Fatalf("missing owner returned chronology: %+v,%v", out, err)
		}
	}
}

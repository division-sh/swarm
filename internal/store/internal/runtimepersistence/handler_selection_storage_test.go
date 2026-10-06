package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestHandlerSelectionStoragePreservesExactColumnsAndScopesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(deliveryReadProjectionStore)
			ctx, runID, otherRun := testAuthorActivityContext(), uuid.NewString(), uuid.NewString()
			var exact events.Event
			want := make(map[string]HandlerSelectionStorageEvidence)
			for _, run := range []string{runID, otherRun} {
				seedAuthorActivityReceiptRun(t, fixture, ctx, run)
				event := eventtest.PersistedProjection(uuid.NewString(), "selection.storage", "gateway", "", []byte(`{}`), 0, run, "", events.EventEnvelope{}, time.Now().UTC())
				if run == runID {
					exact = event
				}
				routes := []events.DeliveryRoute{testEntitylessNodeDeliveryRoute("first"), testEntitylessNodeDeliveryRoute("second"),
					{Recipient: events.MustAgentDeliveryRecipient(uuid.NewString())}}
				if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes); err != nil {
					t.Fatal(err)
				}
				if rows, err := ReadHandlerSelectionStorageForTest(ctx, fixture.store, event.ID()); err != nil || len(rows) != 0 {
					t.Fatalf("pending delivery fabricated final selection: %#v %v", rows, err)
				}
				for index, route := range routes {
					claimed, err := claimDeliveryFixture(ctx, selected, event, route)
					if err != nil {
						t.Fatal(err)
					}
					fact := handlerselection.NotApplicable()
					if route.Recipient.IsNode() {
						ref, err := runtimeidentity.AdmitDeclarationIdentity(".", "handler_rule", `nodes["worker"].handlers["selection.storage"].rules[0]`)
						if err != nil {
							t.Fatal(err)
						}
						label := "primary"
						if index == 1 {
							label = "sibling"
						}
						if run == otherRun {
							label = "other-run"
						}
						fact, err = handlerselection.Selected(handlerselection.ContextRules, ref, label)
						if err != nil {
							t.Fatal(err)
						}
						if run == runID {
							want[claimed.Snapshot.DeliveryID] = HandlerSelectionStorageEvidence{DeliveryID: claimed.Snapshot.DeliveryID,
								Context: string(fact.Context()), Disposition: string(fact.Disposition()), FlowPath: ref.Flow().String(),
								Family: ref.Family(), SemanticPath: ref.SemanticPath(), DisplayLabel: fact.DisplayLabel()}
						}
					}
					if _, err := selected.SettleSuccess(ctx, claimed.Claim, nil, 0, fact); err != nil {
						t.Fatal(err)
					}
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			rows, err := ReadHandlerSelectionStorageForTest(ctx, fixture.store, exact.ID())
			if err != nil || len(rows) != len(want) {
				t.Fatalf("node selection cardinality: %#v %v, want %#v", rows, err, want)
			}
			for _, row := range rows {
				if expected, ok := want[row.DeliveryID]; !ok || row != expected {
					t.Fatalf("wrong event/recipient or changed storage: %#v, want %#v", row, want)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("selection readback bypassed original snapshot: %+v", counts)
			}
			if got, err := ReadHandlerSelectionStorageForTest(ctx, fixture.store, uuid.NewString()); err != nil || len(got) != 0 {
				t.Fatalf("absent event fabricated evidence: %#v %v", got, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadHandlerSelectionStorageForTest(cancelled, fixture.store, exact.ID()); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled read returned evidence: %#v %v", got, err)
			}
			// A physical storage witness must not hide noncanonical stored text
			// behind the canonical decoder's label normalization.
			id := rows[0].DeliveryID
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				result, err := tx.ExecContext(ctx, `UPDATE event_delivery_handler_rule_selections SET display_label='  exact physical label  ' WHERE delivery_id=$1`, id)
				if err != nil {
					return err
				}
				count, err := result.RowsAffected()
				if err != nil || count != 1 {
					return fmt.Errorf("physical label control changed %d rows: %v", count, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			changed := want[id]
			changed.DisplayLabel = "  exact physical label  "
			want[id] = changed
			rows, err = ReadHandlerSelectionStorageForTest(ctx, fixture.store, exact.ID())
			if err != nil || len(rows) != len(want) {
				t.Fatalf("physical text read: %#v %v", rows, err)
			}
			for _, row := range rows {
				if expected, ok := want[row.DeliveryID]; !ok || row != expected {
					t.Fatalf("readback normalized stored columns: %#v, want %#v", row, want)
				}
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadHandlerSelectionStorageForTest(ctx, fixture.store, exact.ID()); err == nil || got != nil {
				t.Fatalf("closed owner returned evidence: %#v %v", got, err)
			}
		})
	}
}

func TestHandlerSelectionStorageRejectsUnavailableAndInvalidOwners(t *testing.T) {
	ctx, eventID := context.Background(), uuid.NewString()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadHandlerSelectionStorageForTest(ctx, owner, eventID); err == nil || got != nil {
			t.Fatalf("foreign owner %T returned evidence: %#v %v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, invalid := range []string{"", "invalid", uuid.Nil.String()} {
				if got, err := ReadHandlerSelectionStorageForTest(ctx, fixture.store, invalid); err == nil || got != nil {
					t.Fatalf("invalid identity returned evidence: %#v %v", got, err)
				}
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE event_delivery_handler_rule_selections RENAME TO unavailable_handler_selections`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_handler_selections RENAME TO event_delivery_handler_rule_selections`)
					return err
				}); err != nil {
					t.Error(err)
				}
			}()
			if got, err := ReadHandlerSelectionStorageForTest(ctx, fixture.store, eventID); err == nil || got != nil {
				t.Fatalf("unavailable table returned partial evidence: %#v %v", got, err)
			}
		})
	}
}

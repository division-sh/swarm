package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
)

// Invariant/reach: detached diagnostic attribution must prove the exact native
// event/delivery/run join, including runless platform and substitution negatives.
func TestEventDeliveryRunReadRequiresExactNativeOwnershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, adapter, _ := selectionWriterFixture(t, backend)
			idType := "TEXT"
			if backend == "postgres" {
				idType = "UUID"
			}
			for _, ddl := range []string{
				`CREATE TABLE events (event_id ` + idType + ` PRIMARY KEY, run_id ` + idType + `, event_class TEXT, produced_by_type TEXT, source_event_id ` + idType + `)`,
				`CREATE TABLE event_deliveries (delivery_id ` + idType + ` PRIMARY KEY, event_id ` + idType + `, run_id ` + idType + `)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			runID, foreignRun, eventID, otherEvent, deliveryID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			event := func(run string) events.Event {
				return eventtest.RuntimeControl(eventID, "platform.boot", "runtime", "", []byte(`{}`), 0, run, "", events.EventEnvelope{}, time.Now().UTC())
			}
			for _, tc := range []struct {
				name, mutation string
				args           []any
				eventRun       string
				selected       string
				valid          bool
			}{
				{name: "runless_platform", selected: runID, valid: true},
				{name: "runtime_diagnostic", mutation: `UPDATE events SET event_class='runtime_diagnostic'`, selected: runID, valid: true},
				{name: "ordinary_exact", mutation: `UPDATE events SET run_id=$1`, args: []any{runID}, eventRun: runID, selected: runID, valid: true},
				{name: "missing_event", mutation: `DELETE FROM events`, selected: runID},
				{name: "missing_delivery", mutation: `DELETE FROM event_deliveries`, selected: runID},
				{name: "substituted_event", mutation: `UPDATE event_deliveries SET event_id=$1`, args: []any{otherEvent}, selected: runID},
				{name: "foreign_delivery_run", mutation: `UPDATE event_deliveries SET run_id=$1`, args: []any{foreignRun}, selected: runID},
				{name: "foreign_publication_run", mutation: `UPDATE events SET run_id=$1`, args: []any{foreignRun}, eventRun: runID, selected: runID},
				{name: "foreign_caller_run", eventRun: foreignRun, selected: runID},
				{name: "missing_selected_run"},
				{name: "non_platform", mutation: `UPDATE events SET produced_by_type='external'`, selected: runID},
				{name: "non_runtime_class", mutation: `UPDATE events SET event_class='authored'`, selected: runID},
				{name: "causal_event", mutation: `UPDATE events SET source_event_id=$1`, args: []any{otherEvent}, selected: runID},
			} {
				t.Run(tc.name, func(t *testing.T) {
					for _, statement := range []string{`DELETE FROM event_deliveries`, `DELETE FROM events`} {
						if _, err := db.Exec(statement); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := db.Exec(`INSERT INTO events VALUES ($1,NULL,'runtime_control','platform',NULL),($2,NULL,'runtime_control','platform',NULL)`, eventID, otherEvent); err != nil {
						t.Fatal(err)
					}
					if _, err := db.Exec(`INSERT INTO event_deliveries VALUES ($1,$2,$3)`, deliveryID, eventID, runID); err != nil {
						t.Fatal(err)
					}
					if tc.mutation != "" {
						if _, err := db.Exec(tc.mutation, tc.args...); err != nil {
							t.Fatal(err)
						}
					}
					got, err := adapter.EventDeliveryRunID(t.Context(), db, event(tc.eventRun), deliveryID, tc.selected)
					if tc.valid && (err != nil || got != runID) {
						t.Fatalf("exact event/delivery attribution=%q err=%v", got, err)
					}
					if !tc.valid && (err == nil || got != "") {
						t.Fatalf("foreign/missing attribution accepted=%q err=%v", got, err)
					}
				})
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if got, err := adapter.EventDeliveryRunID(ctx, db, event(""), deliveryID, runID); got != "" || !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled read returned attribution=%q err=%v", got, err)
			}
		})
	}
}

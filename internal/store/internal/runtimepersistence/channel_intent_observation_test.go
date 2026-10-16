package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/google/uuid"
)

// Intent-owner predicate controls, not provider admission or execution proof.
// Native intent writers retain valid facts; no table INSERT/UPDATE is seeded.
func TestChannelIntentObservationRequiresCompleteIndependentIdentityBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, kind := range []render.IntentKind{render.IntentAction, render.IntentText} {
				t.Run(string(kind), func(t *testing.T) {
					for _, mode := range []string{"foreign_provider_only", "foreign_interface_only", "absent", "pending_beside_foreign", "settled_exact"} {
						t.Run(mode, func(t *testing.T) {
							proveExactIntentObservation(t, f, kind, mode, backend.name == "postgres")
						})
					}
				})
			}
		})
	}
}

func proveExactIntentObservation(t *testing.T, f authorActivityReceiptFixture, kind render.IntentKind, mode string, postgres bool) {
	t.Helper()
	ctx := context.Background()
	identity := operatorChannelContractIdentity("observation-target")
	foreign := operatorChannelContractIdentity("observation-foreign")
	eventID := uuid.NewString()
	query := render.IntentObservationQuery{Kind: kind, Provider: "telegram", ProviderEventID: eventID, InterfaceKey: identity.Key()}
	if mode == "foreign_provider_only" || mode == "pending_beside_foreign" || mode == "settled_exact" {
		insertObservedIntent(t, f, kind, "foreign-provider", eventID, identity, true, postgres)
	}
	if mode == "foreign_interface_only" || mode == "pending_beside_foreign" || mode == "settled_exact" {
		insertObservedIntent(t, f, kind, query.Provider, eventID, foreign, true, postgres)
	}
	if mode == "pending_beside_foreign" || mode == "settled_exact" {
		insertObservedIntent(t, f, kind, query.Provider, eventID, identity, mode == "settled_exact", postgres)
	}
	result, found, err := ObserveChannelIntentForTest(ctx, f.store, query)
	if err != nil {
		t.Fatal(err)
	}
	wantFound := mode == "pending_beside_foreign" || mode == "settled_exact"
	if found != wantFound {
		t.Fatalf("foreign occurrence satisfied exact identity: mode=%s got=%+v found=%t", mode, result, found)
	}
	if !wantFound && result != (render.IntentObservation{}) {
		t.Fatal("absent exact occurrence returned foreign facts:", result)
	}
	if mode == "pending_beside_foreign" && (result.State != "pending" || result.Disposition != "") {
		t.Fatal("settled foreign occurrence satisfied pending target:", result)
	}
	if mode == "settled_exact" && (result.State != "settled" || result.Disposition != intentObservationDisposition(kind)) {
		t.Fatal("exact native settlement was not visible:", result)
	}
	if mode == "foreign_provider_only" || mode == "foreign_interface_only" {
		// The original first-row predicate falsely satisfied this assertion;
		// the exact canonical observer must leave it pending/absent instead.
		state, disposition := oldEventOnlyIntentObservation(t, f.db, kind, eventID)
		if state != "settled" || disposition != intentObservationDisposition(kind) || found {
			t.Fatal("foreign-settlement counterexample no longer discriminates:", state, disposition, found)
		}
	}
}

func insertObservedIntent(t *testing.T, f authorActivityReceiptFixture, kind render.IntentKind, provider, eventID string,
	identity operatorchannel.InterfaceIdentity, settled, postgres bool) {
	t.Helper()
	ctx := context.Background()
	text := operatorchannel.InboundText{
		TextFact: operatorchannel.TextFact{Interface: identity, ExternalAccountRef: "observation-account", ConversationRef: "observation-room",
			ConversationScope: operatorchannel.ConversationScopeDirect, Text: "observer proof", MessageReference: `{"id":"observer-proof"}`},
		Provider: provider, ProviderEventID: eventID, PublicationID: uuid.NewString(), ProviderAuthorization: "intent-owner-test-fact",
	}
	err := runUnrevisionedEventFixtureTransactionForTest(ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
		if kind == render.IntentText {
			if err := channeldelivery.InsertTextIntentTx(ctx, tx, text, time.Now(), postgres); err != nil {
				return err
			}
			if settled {
				return channeldelivery.SettleTextIntentTx(ctx, tx, text, intentObservationDisposition(kind), postgres)
			}
			return nil
		}
		action := operatorchannel.InboundAction{
			ActionFact: operatorchannel.ActionFact{Kind: operatorchannel.ActionSourceCallback, Interface: identity,
				ExternalAccountRef: text.ExternalAccountRef, ConversationRef: text.ConversationRef, ConversationScope: text.ConversationScope,
				MessageReference: text.MessageReference, Token: "observer-control", InteractionRef: "observer-interaction"},
			Provider: provider, ProviderEventID: eventID, PublicationID: text.PublicationID, ProviderAuthorization: text.ProviderAuthorization,
		}
		if err := channeldelivery.InsertActionIntentTx(ctx, tx, action, time.Now(), postgres); err != nil {
			return err
		}
		if settled {
			_, err := channeldelivery.SettleUnappliedActionIntentTx(ctx, tx, action, render.ActionRejected, postgres)
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func intentObservationDisposition(kind render.IntentKind) string {
	if kind == render.IntentAction {
		return "rejected"
	}
	return "teaching"
}

func oldEventOnlyIntentObservation(t *testing.T, db *sql.DB, kind render.IntentKind, eventID string) (string, string) {
	t.Helper()
	query := `SELECT state,COALESCE(disposition,'') FROM operator_channel_text_intents WHERE provider_event_id=$1`
	if kind == render.IntentAction {
		query = `SELECT state,COALESCE(disposition,'') FROM operator_channel_action_intents WHERE provider_event_id=$1`
	}
	var state, disposition string
	if err := db.QueryRow(query, eventID).Scan(&state, &disposition); err != nil {
		t.Fatal(err)
	}
	return state, disposition
}

func TestChannelIntentObservationRefusesInvalidCancelledClosedAndDuplicateEvidenceBothStores(t *testing.T) {
	ctx := context.Background()
	query := render.IntentObservationQuery{Kind: render.IntentText, Provider: "telegram", ProviderEventID: uuid.NewString(), InterfaceKey: operatorChannelContractIdentity("observation").Key()}
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if result, found, err := ObserveChannelIntentForTest(ctx, selected, query); err == nil || found || result != (render.IntentObservation{}) {
			t.Fatal("invalid observation owner returned facts:", result, found, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, field := range []string{"kind", "provider", "event", "interface"} {
				invalid := incompleteIntentIdentity(query, field)
				if result, found, err := ObserveChannelIntentForTest(ctx, f.store, invalid); err == nil || found || result != (render.IntentObservation{}) {
					t.Fatal("incomplete identity produced intent evidence:", field, result, found, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if result, found, err := ObserveChannelIntentForTest(cancelled, f.store, query); !errors.Is(err, context.Canceled) || found || result != (render.IntentObservation{}) {
				t.Fatal("cancelled observer returned evidence:", result, found, err)
			}
			requireDuplicateIntentIdentityRefusal(t, f, query.Provider, backend.name == "postgres")
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if result, found, err := ObserveChannelIntentForTest(ctx, f.store, query); err == nil || found || result != (render.IntentObservation{}) {
				t.Fatal("closed observer returned evidence:", result, found, err)
			}
		})
	}
}

func requireDuplicateIntentIdentityRefusal(t *testing.T, f authorActivityReceiptFixture, provider string, postgres bool) {
	t.Helper()
	for _, kind := range []render.IntentKind{render.IntentText, render.IntentAction} {
		identity := operatorChannelContractIdentity("duplicate-observation")
		eventID := uuid.NewString()
		for index := 0; index < 2; index++ {
			insertObservedIntent(t, f, kind, provider, eventID, identity, false, postgres)
		}
		duplicate := render.IntentObservationQuery{Kind: kind, Provider: provider, ProviderEventID: eventID, InterfaceKey: identity.Key()}
		result, found, err := ObserveChannelIntentForTest(context.Background(), f.store, duplicate)
		if err == nil || !strings.Contains(err.Error(), "channel intent observation identity is not unique") || found || result != (render.IntentObservation{}) {
			t.Fatal("duplicate observation identity returned first-row evidence:", result, found, err)
		}
	}
}

func incompleteIntentIdentity(query render.IntentObservationQuery, field string) render.IntentObservationQuery {
	switch field {
	case "kind":
		query.Kind = "foreign"
	case "provider":
		query.Provider = " "
	case "event":
		query.ProviderEventID = " "
	case "interface":
		query.InterfaceKey = ""
	default:
		panic("unclassified incomplete intent identity")
	}
	return query
}

func TestChannelIntentObservationSeparatesReusedTokenAndDistinctOccurrencesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			identity := operatorChannelContractIdentity("occurrence-observation")
			ctx, postgres := context.Background(), backend.name == "postgres"
			old, next := uuid.NewString(), uuid.NewString()
			insertObservedIntent(t, f, render.IntentAction, "telegram", old, identity, true, postgres)
			insertObservedIntent(t, f, render.IntentAction, "telegram", next, identity, false, postgres)
			query := render.IntentObservationQuery{Kind: render.IntentAction, Provider: "telegram", ProviderEventID: next, InterfaceKey: identity.Key()}
			result, found, err := ObserveChannelIntentForTest(ctx, f.store, query)
			if err != nil || !found || result.State != "pending" || result.Disposition != "" {
				t.Fatal("old settled control token satisfied new pending occurrence:", result, found, err)
			}
			var oldTokenMatches int
			if err := f.db.QueryRow(`SELECT COUNT(*) FROM operator_channel_action_intents WHERE state='settled'
				AND disposition IN ('stale','rejected') AND CAST(fact AS TEXT) LIKE $1`, "%observer-control%").Scan(&oldTokenMatches); err != nil || oldTokenMatches != 1 {
				t.Fatal("original token predicate no longer demonstrates the false settlement:", oldTokenMatches, err)
			}
			a, b := uuid.NewString(), uuid.NewString()
			insertObservedIntent(t, f, render.IntentAction, "telegram", a, identity, true, postgres)
			insertObservedIntent(t, f, render.IntentAction, "foreign-provider", a, identity, true, postgres)
			var combined int
			if err := f.db.QueryRow(`SELECT COUNT(*) FROM operator_channel_action_intents WHERE state='settled'
				AND disposition IN ('stale','rejected') AND provider_event_id IN ($1,$2)`, a, b).Scan(&combined); err != nil || combined != 2 {
				t.Fatal("original two-row predicate no longer demonstrates missing B:", combined, err)
			}
			want := map[string]render.IntentObservation{a: {State: "settled", Disposition: "rejected"}, b: {}}
			for _, occurrence := range []string{a, b} {
				query.ProviderEventID = occurrence
				result, found, err := ObserveChannelIntentForTest(ctx, f.store, query)
				if err != nil || found != (occurrence == a) || result != want[occurrence] {
					t.Fatal("two A rows substituted for independent B settlement:", occurrence, result, found, err)
				}
			}
			insertObservedIntent(t, f, render.IntentAction, "telegram", b, identity, true, postgres)
			for _, occurrence := range []string{a, b} {
				query.ProviderEventID = occurrence
				result, found, err := ObserveChannelIntentForTest(ctx, f.store, query)
				if err != nil || !found || result.State != "settled" || result.Disposition != "rejected" {
					t.Fatal("distinct native A/B settlements not observed:", occurrence, result, found, err)
				}
			}
		})
	}
}

package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
)

func TestRootConnectedTemplatePublicationAcknowledgmentLossBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			fault := &rootPublicationAckLoss{}
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				persistence := previous(owner)
				switch selected := persistence.deps.EventStore.(type) {
				case *store.PostgresStore:
					persistence.deps.EventStore = &rootPublicationPostgresAckLoss{PostgresStore: selected, fault: fault}
				case *store.SQLiteRuntimeStore:
					persistence.deps.EventStore = &rootPublicationSQLiteAckLoss{SQLiteRuntimeStore: selected, fault: fault}
				default:
					t.Fatalf("unexpected selected store %T", selected)
				}
				return persistence
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			endpoint, db, bundleHash := startPublicInputRollbackRuntime(t, backend)
			params := map[string]any{
				"bundle_hash": bundleHash, "event_name": "inbound.telegram.text_message",
				"payload":         map[string]any{"conversation_reference": "42", "conversation_scope": "direct", "external_account_reference": "42", "provider_message_reference": 1, "text": "acknowledgment proof"},
				"idempotency_key": "root-connected-ack-loss",
			}
			first := requireServedJSONRPCError(t, endpoint, "event.publish", params)
			if first.Data["code"] != apiv1.EventPublishFailedCode {
				t.Fatalf("unexpected acknowledgment failure: %#v", first)
			}
			fault.mu.Lock()
			stored := append([]byte(nil), fault.completion...)
			fault.mu.Unlock()
			var want, got map[string]any
			if err := json.Unmarshal(stored, &want); err != nil {
				t.Fatalf("missing committed completion: %v", err)
			}
			if deliveries, _ := want["deliveries"].([]any); len(deliveries) == 0 {
				t.Fatalf("fault never reached connected private delivery: %s", stored)
			}
			for attempt := 0; attempt < 2; attempt++ {
				requireServedJSONRPCResult(t, endpoint, "event.publish", params, &got)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("keyed reconciliation changed committed result: got=%#v want=%#v", got, want)
				}
			}
			fault.mu.Lock()
			commits := fault.commits
			fault.mu.Unlock()
			if commits != 1 {
				t.Fatalf("same-key replay attempted %d publication commits", commits)
			}
			for label, statement := range map[string]string{
				"private instance": "SELECT COUNT(*) FROM flow_instances WHERE flow_template = 'telegram-chat'",
				"root event":       "SELECT COUNT(*) FROM events WHERE event_name = 'inbound.telegram.text_message'",
				"API completion":   "SELECT COUNT(*) FROM api_idempotency WHERE method = 'event.publish' AND idempotency_key = 'root-connected-ack-loss'",
			} {
				var count int
				if err := db.QueryRowContext(context.Background(), statement).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("%s count=%d, want one committed fact", label, count)
				}
			}
		})
	}
}

type rootPublicationAckLoss struct {
	mu         sync.Mutex
	commits    int
	completion []byte
}

func (f *rootPublicationAckLoss) commit(ctx context.Context, owner runtimebus.APIEventPublicationCommitOwner, command runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	committed, err := owner.CommitAPIEventPublication(ctx, command)
	if err != nil {
		return committed, err
	}
	f.completion = append([]byte(nil), committed.Completion.Response...)
	return runtimebus.CommittedAPIEventPublication{}, errors.New("injected acknowledgment loss after selected-store publication commit")
}

type rootPublicationPostgresAckLoss struct {
	*store.PostgresStore
	fault *rootPublicationAckLoss
}

func (s *rootPublicationPostgresAckLoss) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	return s.fault.commit(ctx, s.PostgresStore, command)
}

type rootPublicationSQLiteAckLoss struct {
	*store.SQLiteRuntimeStore
	fault *rootPublicationAckLoss
}

func (s *rootPublicationSQLiteAckLoss) CommitAPIEventPublication(ctx context.Context, command runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	return s.fault.commit(ctx, s.SQLiteRuntimeStore, command)
}

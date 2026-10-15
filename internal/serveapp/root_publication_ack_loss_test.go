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
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestRootConnectedTemplatePublicationAcknowledgmentLossBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			fault := &rootPublicationAckLoss{}
			var reader receiverProofStateReader
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				persistence := previous(owner)
				reader = requireReceiverProofStateReader(t, persistence.deps.EventStore)
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
			endpoint, _, bundleHash := startPublicInputRollbackRuntime(t, backend)
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
			for label, read := range map[string]func(context.Context) (int, error){
				"private instance": func(ctx context.Context) (int, error) {
					return storetest.CountTemplateInstances(ctx, reader, "telegram-chat")
				},
				"root event": func(ctx context.Context) (int, error) {
					return storetest.CountEventNameStorage(ctx, reader, "inbound.telegram.text_message")
				},
				"API completion": func(ctx context.Context) (int, error) {
					return storetest.ReadAPIIdempotencyCardinality(ctx, reader, "event.publish", "root-connected-ack-loss")
				},
			} {
				var count int
				if err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
					var err error
					count, err = read(ctx)
					return err
				}); err != nil {
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

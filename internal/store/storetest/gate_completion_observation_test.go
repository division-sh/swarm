package storetest

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/google/uuid"
)

func TestGateCompletionAPIWitnessKeepsCompetingActorsBothORArmsAndOrderBothStores(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			writer := selected.(interface {
				WithAPIIdempotency(context.Context, apiidempotency.Request, func(context.Context) (apiidempotency.Completion, error)) (apiidempotency.Completion, bool, error)
			})
			card, other := uuid.NewString(), uuid.NewString()
			type seed struct{ method, actor, key, resource, hash string }
			seeds := []seed{
				{"event.publish", "z-actor", "other-key", card, "hash-one"},
				{"event.publish", "a-competing-actor", "target-key", other, "hash-two"},
				{"fixture.other_method", "b-actor", "other-key", card, "hash-three"},
				{"event.publish", "excluded-actor", "other-key", other, "hash-four"},
			}
			for _, seed := range seeds {
				req := apiidempotency.Request{Method: seed.method, Actor: apiidempotency.BearerActor(seed.actor), IdempotencyKey: seed.key, RequestHash: seed.hash, ResourceID: seed.resource, Now: time.Now().UTC(), TTL: time.Hour}
				if _, replay, err := writer.WithAPIIdempotency(context.Background(), req, func(context.Context) (apiidempotency.Completion, error) {
					return apiidempotency.Completion{ResourceID: seed.resource, Response: json.RawMessage(`{"retained":true}`)}, nil
				}); err != nil || replay {
					t.Fatalf("native receipt: replay=%t err=%v", replay, err)
				}
			}
			rows, err := ReadGateCompletionAPIStorage(context.Background(), selected, card, "event.publish", "target-key")
			if err != nil || len(rows) != 3 {
				t.Fatalf("broad OR witness lost rows: %+v/%v", rows, err)
			}
			response := `{"retained":true}`
			if backend.name == "postgres" {
				response = `{"retained": true}`
			}
			for i, seed := range []seed{seeds[1], seeds[0], seeds[2]} {
				want := GateCompletionAPIStorage{Method: seed.method, ActorKind: string(apiidempotency.ActorBearerToken), Actor: seed.actor, Key: seed.key, Hash: seed.hash, ResourceID: seed.resource, Response: response}
				if !reflect.DeepEqual(rows[i], want) {
					t.Fatalf("full actor/domain/text/order witness %d=%+v want=%+v", i, rows[i], want)
				}
			}
		})
	}
}

func TestGateCompletionReadPortsKeepDistinctCutsAndRefuseForeignEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner := openReadbackControlInspection(t, backend)
			probe := CollectTransactions(t, owner, TransactionProbeOptions{})
			cuts := gateCompletionReadbackControls(uuid.NewString())
			assertReadbackEmptyAndForeignCuts(t, owner, cuts)
			if got := probe.Snapshot(); got.Active != 0 || got.Total.ReadCommits != uint64(len(cuts)) || got.Total.WriteCommits != 0 {
				t.Fatalf("gate completion conflated its separate read cuts: %+v", got)
			}
			assertConnectionReadbackLifetimeRefusal(t, owner, cuts)
		})
	}
}

func gateCompletionReadbackControls(id string) []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadGateCompletionPrincipalStorage(ctx, selected)
			return value == "", err
		},
		func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadGateCompletionCardStorage(ctx, selected, id)
			return value == (GateCompletionCardStorage{}), err
		},
		func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadGateCompletionFlowStorage(ctx, selected, id)
			return value == (GateCompletionFlowStorage{}), err
		},
		func(ctx context.Context, selected any) (bool, error) {
			rows, err := ReadGateCompletionChangesStorage(ctx, selected, id)
			return len(rows) == 0, err
		},
		func(ctx context.Context, selected any) (bool, error) {
			rows, err := ReadGateCompletionReceiptsStorage(ctx, selected, id)
			return len(rows) == 0, err
		},
		func(ctx context.Context, selected any) (bool, error) {
			rows, err := ReadGateCompletionAPIStorage(ctx, selected, id, "mailbox.decide", "exact-key")
			return len(rows) == 0, err
		},
	}
}

func TestHumanTaskToolReadPortsRefuseForeignAndUnavailableOwnersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner := openReadbackControlInspection(t, backend)
			id := uuid.NewString()
			cuts := []func(context.Context, any) (bool, error){
				func(ctx context.Context, selected any) (bool, error) {
					count, err := CountHumanTaskToolCardStorage(ctx, selected, id)
					return count == 0, err
				},
				func(ctx context.Context, selected any) (bool, error) {
					count, err := CountHumanTaskToolContinuationStorage(ctx, selected, id)
					return count == 0, err
				},
				func(ctx context.Context, selected any) (bool, error) {
					count, err := CountAllHumanTaskToolCardsStorage(ctx, selected)
					return count == 0, err
				},
			}
			assertReadbackEmptyAndForeignCuts(t, owner, cuts)
			assertConnectionReadbackLifetimeRefusal(t, owner, cuts)
		})
	}
}

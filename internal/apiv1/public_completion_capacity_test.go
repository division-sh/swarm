package apiv1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// The cut follows the real native ACK, not a fabricated completion or held SQL
// transaction. It keeps the original admission live across two EventBuses.
type publicCompletionNativeCut struct {
	mu      sync.Mutex
	keys    map[string]chan struct{}
	entered chan string
	calls   int
	err     error
	stored  map[string]json.RawMessage
}

func (p *publicCompletionNativeCut) await(ctx context.Context, key string, acknowledged bool, completion apiidempotency.Completion, err error) {
	p.mu.Lock()
	p.calls++
	p.err = err
	p.stored[key] = append(json.RawMessage(nil), completion.Response...)
	release := p.keys[key]
	p.mu.Unlock()
	if !acknowledged || release == nil {
		return
	}
	p.entered <- key
	select {
	case <-release:
	case <-ctx.Done():
	}
}

type publicCompletionSQLiteCut struct {
	*store.SQLiteRuntimeStore
	cut *publicCompletionNativeCut
}

func (s *publicCompletionSQLiteCut) CommitAPIEventPublication(ctx context.Context, cmd runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	result, err := s.SQLiteRuntimeStore.CommitAPIEventPublication(ctx, cmd)
	s.cut.await(ctx, cmd.Idempotency.IdempotencyKey, result.Acknowledged, result.Completion, err)
	return result, err
}

func (s *publicCompletionSQLiteCut) CommitDeploymentRunCreation(ctx context.Context, cmd runtimebus.DeploymentRunCreationCommand) (runtimebus.CommittedDeploymentRunCreation, error) {
	result, err := s.SQLiteRuntimeStore.CommitDeploymentRunCreation(ctx, cmd)
	s.cut.await(ctx, cmd.Idempotency.IdempotencyKey, result.Acknowledged, result.Completion, err)
	return result, err
}

type publicCompletionPostgresCut struct {
	*store.PostgresStore
	cut *publicCompletionNativeCut
}

func (s *publicCompletionPostgresCut) CommitAPIEventPublication(ctx context.Context, cmd runtimebus.APIEventPublicationCommand) (runtimebus.CommittedAPIEventPublication, error) {
	result, err := s.PostgresStore.CommitAPIEventPublication(ctx, cmd)
	s.cut.await(ctx, cmd.Idempotency.IdempotencyKey, result.Acknowledged, result.Completion, err)
	return result, err
}

func (s *publicCompletionPostgresCut) CommitDeploymentRunCreation(ctx context.Context, cmd runtimebus.DeploymentRunCreationCommand) (runtimebus.CommittedDeploymentRunCreation, error) {
	result, err := s.PostgresStore.CommitDeploymentRunCreation(ctx, cmd)
	s.cut.await(ctx, cmd.Idempotency.IdempotencyKey, result.Acknowledged, result.Completion, err)
	return result, err
}

func TestPublicCompletionCapacityNativeMixedBothStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		root := canonicalrouting.CopyPublicCompletionInitiation(t)
		repo := canonicalrouting.RepoRoot(t)
		bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatal(err)
		}
		source := semanticview.Wrap(bundle)
		catalog, err := runtimecontracts.BuildDurableDataCatalog(bundle)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := durabledata.ParseDeclarationRef(".", "scan.requested")
		if err != nil {
			t.Fatal(err)
		}
		hash := runStartTestBundleHashForSource(source)
		cut := &publicCompletionNativeCut{keys: make(map[string]chan struct{}), entered: make(chan string, worklifetime.PublicCompletionLimit+1), stored: make(map[string]json.RawMessage)}
		defer func() {
			cut.mu.Lock()
			defer cut.mu.Unlock()
			for _, release := range cut.keys {
				select {
				case <-release:
				default:
					close(release)
				}
			}
		}()
		var selected dataRunLifecycleStore
		switch native := fixture.primary.(type) {
		case *store.SQLiteRuntimeStore:
			selected = &publicCompletionSQLiteCut{SQLiteRuntimeStore: native, cut: cut}
		case *store.PostgresStore:
			selected = &publicCompletionPostgresCut{PostgresStore: native, cut: cut}
		default:
			t.Fatalf("unexpected native owner %T", native)
		}
		process := worklifetime.NewProcess()
		var owners []*worklifetime.RuntimeOccurrence
		var handlers []*Handler
		t.Cleanup(func() {
			cut.mu.Lock()
			for _, release := range cut.keys {
				select {
				case <-release:
				default:
					close(release)
				}
			}
			cut.mu.Unlock()
			process.Retire()
			for _, owner := range owners {
				if _, err := owner.RetireAndWait(context.Background()); err != nil {
					t.Errorf("retire exact runtime: %v", err)
				}
			}
			if _, err := process.Join(context.Background()); err != nil {
				t.Errorf("join process: %v", err)
			}
		})
		for range 2 {
			options := runStartTestEventBusOptions(source)
			options.RuntimeInstanceID = uuid.NewString()
			owner, err := process.NewRuntime(t.Context(), worklifetime.RuntimeIdentity{RuntimeInstanceID: options.RuntimeInstanceID, BundleHash: hash})
			if err != nil {
				t.Fatal(err)
			}
			owners = append(owners, owner)
			options.WorkOwner = owner
			bus, err := newScopedAPITestEventBusWithDataCatalog(t, selected, &catalog, options)
			if err != nil {
				t.Fatal(err)
			}
			handlers = append(handlers, eventPublishTestHandlerWithStores(t, selected, selected, selected, bus, source))
		}
		seedRun, sourceID := uuid.NewString(), uuid.NewString()
		seedData := map[string]any{"imports": []any{dataRunFusedImport(sourceID, ref, durabledata.AbsentHead(), []byte("{\"topic\":\"one\"}\n"))}, "pins": []any{}}
		if response := rpcCall(t, handlers[0], deploymentRunStartBody(seedRun, uuid.NewString(), hash, seedData)); response.Error != nil {
			t.Fatalf("admit shared data: %+v native_error=%v", response.Error, cut.err)
		}
		if err := owners[0].WaitForQuiescence(t.Context()); err != nil {
			t.Fatal(err)
		}
		receipt, err := selected.LoadDataSourceOperation(t.Context(), sourceID)
		if err != nil {
			t.Fatal(err)
		}
		data := map[string]any{"imports": []any{}, "pins": []any{map[string]any{"declaration": dataRunDeclaration(ref), "version_id": receipt.Result.Candidate.VersionID}}}
		bodies := make([]string, worklifetime.PublicCompletionLimit+1)
		responses := make([]chan publicCompletionObservedResponse, len(bodies))
		keys := make([]string, len(bodies))
		for index := range bodies {
			keys[index] = uuid.NewString()
			cut.keys[keys[index]] = make(chan struct{})
			switch index {
			case 0:
				bodies[index] = dataRunEventPublishBodyForBundle(seedRun, keys[index], hash, nil)
			case 1:
				bodies[index] = dataRunEventPublishBodyForBundle(uuid.NewString(), keys[index], hash, data)
			case 2:
				bodies[index] = dataRunStartBodyForBundle(uuid.NewString(), keys[index], hash, data)
			default:
				bodies[index] = deploymentRunStartBody(uuid.NewString(), keys[index], hash, data)
			}
			responses[index] = make(chan publicCompletionObservedResponse, 1)
		}
		for index := range worklifetime.PublicCompletionLimit {
			go func() {
				responses[index] <- publicCompletionObservedRPC(t, handlers[index%2], bodies[index], testToken)
			}()
			select {
			case key := <-cut.entered:
				if key != keys[index] {
					t.Fatalf("wrong native cut: %s", key)
				}
			case response := <-responses[index]:
				t.Fatalf("reservation %d never reached real ACK: %+v", index, response.Error)
			case <-time.After(5 * time.Second):
				t.Fatalf("reservation %d did not ACK; pool=%+v", index, storetest.ObserveSelectedPool(t, fixture.primary))
			}
		}
		if count := process.PublicCompletionCount(); count != worklifetime.PublicCompletionLimit {
			t.Fatalf("shared process reservations=%d", count)
		}
		t.Logf("N=%d, native pool=%+v", process.PublicCompletionCount(), storetest.ObserveSelectedPool(t, fixture.primary))
		before, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), fixture.primary)
		if err != nil {
			t.Fatal(err)
		}
		refused := rpcCall(t, handlers[0], bodies[worklifetime.PublicCompletionLimit])
		if refused.Error == nil || asMap(t, refused.Error.Data)["retryable"] != true || !strings.Contains(fmt.Sprint(refused.Error.Data), worklifetime.ErrPublicCompletionCapacity.Error()) {
			t.Fatalf("N+1 did not refuse nonblockingly/retryably: %+v", refused)
		}
		for _, index := range []int{0, 1, 2} {
			var request map[string]any
			if err := json.Unmarshal([]byte(bodies[index]), &request); err != nil {
				t.Fatal(err)
			}
			params := request["params"].(map[string]any)
			params["idempotency_key"] = uuid.NewString()
			if index == 0 {
				params["payload"] = map[string]any{"topic": "capacity-refusal"}
			} else {
				params["run_id"] = uuid.NewString()
			}
			raw, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			response := rpcCall(t, handlers[index%2], string(raw))
			if response.Error == nil || asMap(t, response.Error.Data)["retryable"] != true || !strings.Contains(fmt.Sprint(response.Error.Data), worklifetime.ErrPublicCompletionCapacity.Error()) {
				t.Fatalf("saturated variant %d did not refuse capacity: %+v", index, response.Error)
			}
		}
		for _, index := range []int{0, 1, 2, 3} {
			replay := publicCompletionObservedRPC(t, handlers[(index+1)%2], bodies[index], testToken)
			if replay.Error != nil {
				t.Fatalf("saturated replay variant %d: %+v", index, replay)
			}
			cut.mu.Lock()
			stored := append(json.RawMessage(nil), cut.stored[keys[index]]...)
			cut.mu.Unlock()
			if !bytes.Equal(replay.raw, stored) {
				t.Fatalf("saturated replay variant %d changed native completion bytes: native=%s replay=%s", index, stored, replay.raw)
			}
		}
		after, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), fixture.primary)
		cut.mu.Lock()
		calls := cut.calls
		cut.mu.Unlock()
		if err != nil || before != after || calls != worklifetime.PublicCompletionLimit+1 {
			t.Fatalf("refusal/replay reached mutation: before=%+v after=%+v native calls=%d err=%v", before, after, calls, err)
		}
		close(cut.keys[keys[3]])
		response := <-responses[3]
		cut.mu.Lock()
		stored := append(json.RawMessage(nil), cut.stored[keys[3]]...)
		cut.mu.Unlock()
		if response.Error != nil || !bytes.Equal(response.raw, stored) {
			t.Fatalf("release-one: %+v", response)
		}
		deadline := time.Now().Add(5 * time.Second)
		for process.PublicCompletionCount() != worklifetime.PublicCompletionLimit-1 {
			if time.Now().After(deadline) {
				t.Fatalf("released exact completion retained capacity=%d", process.PublicCompletionCount())
			}
			time.Sleep(time.Millisecond)
		}
		go func() { responses[32] <- publicCompletionObservedRPC(t, handlers[0], bodies[32], testToken) }()
		select {
		case key := <-cut.entered:
			if key != keys[32] {
				t.Fatalf("readmit exact request=%s", key)
			}
		case response := <-responses[32]:
			t.Fatalf("released slot unavailable: %+v", response)
		case <-time.After(5 * time.Second):
			t.Fatal("released slot did not reach native ACK")
		}
		for index := range bodies {
			if index != 3 {
				close(cut.keys[keys[index]])
				response := <-responses[index]
				cut.mu.Lock()
				stored := append(json.RawMessage(nil), cut.stored[keys[index]]...)
				cut.mu.Unlock()
				if response.Error != nil || !bytes.Equal(response.raw, stored) {
					t.Fatalf("exact native completion %d: %+v", index, response)
				}
			}
		}
		for _, owner := range owners {
			if err := owner.WaitForQuiescence(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := owner.RetireAndWait(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := process.Join(t.Context()); err != nil {
			t.Fatal(err)
		}
		if count := process.PublicCompletionCount(); count != 0 {
			t.Fatalf("retained reservations after all dispositions=%d", count)
		}
	})
}

func publicCompletionRPC(t *testing.T, handler *Handler, body string) rpcResponse {
	return publicCompletionRPCWithToken(t, handler, body, testToken)
}

func publicCompletionRPCWithToken(t *testing.T, handler *Handler, body, token string) rpcResponse {
	return publicCompletionObservedRPC(t, handler, body, token).rpcResponse
}

type publicCompletionObservedResponse struct {
	rpcResponse
	raw json.RawMessage
}

func publicCompletionObservedRPC(t *testing.T, handler *Handler, body, token string) publicCompletionObservedResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(recorder, testAuthorActivityRequest(request))
	var response publicCompletionObservedResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Errorf("decode exact request completion: %v", err)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Errorf("decode recorded response bytes: %v", err)
	}
	response.raw = envelope.Result
	return response
}

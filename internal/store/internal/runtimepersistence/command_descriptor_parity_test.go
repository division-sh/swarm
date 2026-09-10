package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

func TestCommandDescriptorBothStoresPreserveSelectionAndRefuseCorruption(t *testing.T) {
	for _, purpose := range []executionposture.Posture{executionposture.Live, executionposture.MockOnly} {
		t.Run(string(purpose), func(t *testing.T) {
			forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
				ctx := testAuthorActivityContext()
				profile, err := selection.ResolveLiveBackend(selection.BackendAnthropic)
				if err != nil {
					t.Fatal(err)
				}
				performance := []byte("def handle(input): return {'text': 'fixture'}\n")
				source := withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
					ID: "descriptor-agent", Identity: testAgentIdentity(t, "descriptor-agent", ""),
					Type: "worker", Role: "test", Model: "custom-at-write", LLMBackend: profile.ID,
					Memory: agentmemory.PlatformDefault(), Config: json.RawMessage(`{}`),
					Mock: mockperformance.Performance{Kind: mockperformance.KindPython, Module: "mocks/worker.py",
						Source: performance, Digest: "sha256:" + runtimeeffects.Fingerprint(performance)},
				})
				before := source
				selected, err := runtimellm.ResolveAgentExecution(purpose, profile, selection.ModelAliases{
					"custom-at-write": {profile.ID: "exact-model-at-write"},
				}, source)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(source, before) {
					t.Fatal("selection changed the authored double")
				}
				if err := agentfixture.UpsertStatic(t, ctx, backend.store, runtimemanager.PersistedAgent{
					Config: selected.Actor, Status: "active",
				}); err != nil {
					t.Fatal(err)
				}
				load := func(t *testing.T) runtimeactors.AgentConfig {
					t.Helper()
					rows, err := backend.store.LoadAgents(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if len(rows) != 1 || rows[0].Config.Identity != source.Identity {
						t.Fatalf("exact selected actor missing: %#v", rows)
					}
					return rows[0].Config
				}
				actor := load(t)
				if actor.ResolvedModel != "exact-model-at-write" || actor.Model != source.Model ||
					actor.ResolvedLLMBackend != selected.Actor.ResolvedLLMBackend ||
					actor.ResolvedLLMProvider != selected.Actor.ResolvedLLMProvider ||
					actor.ResolvedLLMTransport != selected.Actor.ResolvedLLMTransport ||
					actor.ExecutionMode != purpose.RootMode() || !reflect.DeepEqual(actor.Mock, selected.Actor.Mock) {
					t.Fatalf("store changed write-time selection: %#v", actor)
				}
				// No model aliases are supplied to the persisted descriptor reader.
				if _, err := runtimellm.ValidateAgentExecutionDescriptor(profile, actor); err != nil {
					t.Fatal(err)
				}
				readSQL := `SELECT runtime_descriptor FROM agents WHERE run_id = ? AND agent_id = ?`
				writeSQL := `UPDATE agents SET runtime_descriptor = ? WHERE run_id = ? AND agent_id = ?`
				if backend.name == "postgres" {
					readSQL = `SELECT runtime_descriptor FROM agents WHERE run_id = $1::uuid AND agent_id = $2`
					writeSQL = `UPDATE agents SET runtime_descriptor = $1::jsonb WHERE run_id = $2::uuid AND agent_id = $3`
				}
				readRaw := func(t *testing.T) string {
					t.Helper()
					var raw string
					if err := backend.db.QueryRowContext(ctx, readSQL, source.Identity.RunID, source.ID).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					return raw
				}
				writeRaw := func(t *testing.T, raw string) {
					t.Helper()
					result, err := backend.db.ExecContext(ctx, writeSQL, raw, source.Identity.RunID, source.ID)
					if err != nil {
						t.Fatal(err)
					}
					if n, err := result.RowsAffected(); err != nil || n != 1 {
						t.Fatalf("exact descriptor update = %d, %v", n, err)
					}
				}
				original := readRaw(t)
				for _, field := range []string{"resolved_model", "resolved_llm_provider", "resolved_llm_transport"} {
					t.Run(field, func(t *testing.T) {
						var row map[string]json.RawMessage
						if err := json.Unmarshal([]byte(original), &row); err != nil {
							t.Fatal(err)
						}
						delete(row, field)
						raw, err := json.Marshal(row)
						if err != nil {
							t.Fatal(err)
						}
						writeRaw(t, string(raw))
						t.Cleanup(func() { writeRaw(t, original) })
						corrupt := readRaw(t)
						for attempt := 0; attempt < 2; attempt++ {
							stored := load(t)
							if _, err := runtimellm.ValidateAgentExecutionDescriptor(profile, stored); err == nil {
								t.Fatalf("read accepted missing %s", field)
							}
							if readRaw(t) != corrupt {
								t.Fatalf("reader repaired missing %s in storage", field)
							}
						}
					})
				}
			})
		})
	}
}

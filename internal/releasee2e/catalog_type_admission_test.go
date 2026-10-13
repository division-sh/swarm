package releasee2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

const admissionPublicTypes = `scalars:
  Label: text
  ID: uuid
enums:
  Status: {values: [open, closed], default: open}
types:
  Item:
    id: Label
    note: Label?
`

const admissionPublicNodes = `complete-task:
  execution_type: system_node
  subscribes_to: [task.requested, key.observed]
  produces: [key.observed]
  event_handlers:
    task.requested:
      data_accumulation:
        writes:
          - labels
          - lookup
          - enums
          - uuids
          - {op: merge, target: entity.lookup, key: '2', value: {note: 'merged'}}
          - {op: set, target: entity.lookup, key: '10', value: {id: 'ten'}}
      advances_to: collecting
      fan_out:
        items_from: payload.lookup
        as: map_key
        identity: map_key
        emit: {event: key.observed, fields: {member_id: map_key}}
    key.observed:
      join:
        stage: collecting
        members: {from: state.lookup, by: payload.member_id}
        output: payload.member_id
        on_complete: {advances_to: done}
`

func TestCatalogTypeAdmissionPublicRestartBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required", goldenPostgresEnv)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base := filepath.Join(root, backend)
			source := filepath.Join(base, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "tests/tier8-boot-verification/test-boot-success"), source)
			writeReleaseFile(t, filepath.Join(source, "types.yaml"), admissionPublicTypes)
			writeReleaseFile(t, filepath.Join(source, "schema.yaml"), "stages:\n  pending: {}\n  collecting: {}\n  done: {final: true}\npins:\n  inputs: [task.requested]\n")
			fields := "  labels: '[Label]'\n  lookup: map[Label]Item\n  enums: map[Status]Item\n  uuids: map[ID]Item\n"
			writeReleaseFile(t, filepath.Join(source, "events.yaml"), "task.requested:\n"+fields+"key.observed:\n  member_id: Label\n")
			writeReleaseFile(t, filepath.Join(source, "entities.yaml"), "test_entity:\n  labels: '[Label]'\n  lookup: {type: 'map[Label]Item', initial: {}}\n  enums: map[Status]Item\n  uuids: map[ID]Item\n")
			writeReleaseFile(t, filepath.Join(source, "nodes.yaml"), admissionPublicNodes)
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config := filepath.Join(base, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, base, store.passwordEnv, 0)
			var description string
			for _, command := range []string{"verify", "describe"} {
				args := []string{command, "--json", source, "--config", config}
				if command == "verify" {
					args = append(args, "--portable")
				}
				result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, args...)
				if result.err != nil {
					t.Fatalf("public %s: %v\n%s", command, result.err, result.output)
				}
				if command == "describe" {
					description = result.output
				}
			}
			start := func() *releaseServeProcess {
				process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, Source: source, ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env})
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := process.waitReady(ctx); err != nil {
					t.Fatalf("public boot: %v\n%s", err, process.output.String())
				}
				return process
			}
			process := start()
			ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
			defer cancel()
			hash := goldenServedBundleHash(t, process.rpc, "live")
			uuid := "d9428888-122b-11e1-b85c-61cd3cbb3210"
			payload := map[string]any{
				"labels": []any{"first", "second"},
				"lookup": map[string]any{" 2": map[string]any{"id": "space"}, "+2": map[string]any{"id": "plus"}, "-1": map[string]any{"id": "negative"}, "02": map[string]any{"id": "zero"}, "1": map[string]any{"id": "one"}, "10": map[string]any{"id": "old"}, "2": map[string]any{"id": "two"}},
				"enums":  map[string]any{"open": map[string]any{"id": "enum"}},
				"uuids":  map[string]any{uuid: map[string]any{"id": "uuid"}},
			}
			var published struct {
				RunID string `json:"run_id"`
			}
			if err := process.rpc.call(ctx, "event.publish", map[string]any{"bundle_hash": hash, "event_name": "task.requested", "payload": payload, "emitter": "releasee2e", "idempotency_key": "admission-valid"}, &published); err != nil || published.RunID == "" {
				t.Fatalf("public typed ingress: %+v %v", published, err)
			}
			waitForGoldenTerminalRun(t, process, store, published.RunID, 30*time.Second)
			events, err := listGoldenEvents(ctx, process.rpc, published.RunID)
			if err != nil {
				t.Fatal(err)
			}
			var keys []string
			// The public event list is newest-first; compare the actual emission
			// chronology, not a lexical re-sort of the observed payload keys.
			sort.Slice(events, func(i, j int) bool { return events[i].CreatedAt.Before(events[j].CreatedAt) })
			for _, event := range events {
				if event.EventName == "key.observed" {
					keys = append(keys, event.Payload["member_id"].(string))
				}
			}
			if !reflect.DeepEqual(keys, []string{" 2", "+2", "-1", "02", "1", "10", "2"}) {
				t.Fatalf("public fan-out lost exact lexical key order: %v; events=%+v\n%s", keys, events, process.output.String())
			}
			var entity map[string]any
			if err := process.rpc.call(ctx, "entity.get", map[string]any{"entity_id": published.RunID, "run_id": published.RunID}, &entity); err != nil {
				t.Fatal(err)
			}
			stateFields, ok := entity["fields"].(map[string]any)
			if !ok {
				t.Fatalf("public entity fields are not an object: %#v", entity)
			}
			lookup, _ := stateFields["lookup"].(map[string]any)
			wantLookup := map[string]any{" 2": map[string]any{"id": "space"}, "+2": map[string]any{"id": "plus"}, "-1": map[string]any{"id": "negative"}, "02": map[string]any{"id": "zero"}, "1": map[string]any{"id": "one"}, "10": map[string]any{"id": "ten"}, "2": map[string]any{"id": "two", "note": "merged"}}
			if !reflect.DeepEqual(lookup, wantLookup) || !reflect.DeepEqual(stateFields["enums"], payload["enums"]) || !reflect.DeepEqual(stateFields["uuids"], payload["uuids"]) {
				t.Fatalf("public whole-map/contained mutation lost typed keys: %#v", stateFields)
			}
			assertCatalogAdmissionJoin(t, entity, []string{" 2", "+2", "-1", "02", "1", "10", "2"})
			before, _ := json.Marshal(entity)
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			process = start()
			if got := goldenServedBundleHash(t, process.rpc, "live"); got != hash {
				t.Fatalf("restart source identity changed: %s != %s", got, hash)
			}
			if err := process.rpc.call(ctx, "entity.get", map[string]any{"entity_id": published.RunID, "run_id": published.RunID}, &entity); err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(entity)
			if !bytes.Equal(before, after) {
				t.Fatalf("restart changed public typed map state: %s != %s", before, after)
			}
			result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "describe", "--json", source, "--config", config)
			if result.err != nil || result.output != description {
				t.Fatalf("restart changed public structural projection: %v", result.err)
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			for index, invalid := range []struct{ declaration, reason string }{
				{"next: Unused", "recursive catalog type"},
				{"missing: Missing", "unknown catalog type"},
				{"keys: map[integer]text", "map keys must be text, enum or uuid"},
				{"keys: map[numeric]text", "map keys must be text, enum or uuid"},
				{"keys: map[boolean]text", "map keys must be text, enum or uuid"},
				{"items: text[]", "is retired; use [text]"},
				{"items: '[]text'", "is retired; use [text]"},
			} {
				bad := filepath.Join(base, "refusal-"+string(rune('a'+index)))
				copyReleaseTree(t, source, bad)
				writeReleaseFile(t, filepath.Join(bad, "unused/schema.yaml"), "name: unused\nstages: {done: {final: true}}\n")
				writeReleaseFile(t, filepath.Join(bad, "unused/types.yaml"), "types:\n  Unused:\n    "+invalid.declaration+"\n")
				for _, command := range []string{"verify", "describe", "serve"} {
					args := []string{command, bad, "--config", config}
					if command != "serve" {
						args = append(args, "--json")
						if command == "verify" {
							args = append(args, "--portable")
						}
					}
					refused := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, args...)
					located := strings.Contains(refused.output, "unused/types.yaml") && strings.Contains(refused.output, invalid.reason)
					if command == "verify" {
						var report struct {
							Errors []struct {
								CheckID  string `json:"check_id"`
								Severity string `json:"severity"`
								Message  string `json:"message"`
							} `json:"errors"`
						}
						if err := json.Unmarshal([]byte(refused.output), &report); err != nil {
							t.Fatal(err)
						}
						located = len(report.Errors) == 1 && report.Errors[0].CheckID == "source_loading" && report.Errors[0].Severity == "hard_invalidity" && strings.Contains(report.Errors[0].Message, "unused/types.yaml") && strings.Contains(report.Errors[0].Message, invalid.reason)
					}
					if refused.err == nil || !located {
						t.Fatalf("%s lacked the exact source admission refusal for %s: %v\n%s", command, invalid.declaration, refused.err, refused.output)
					}
				}
			}
		})
	}
}

func TestCatalogTypeAdmissionTemplateRoutingBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required", goldenPostgresEnv)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base := filepath.Join(root, backend)
			source := filepath.Join(base, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "examples/routing/template-select-or-create"), source)
			writeReleaseFile(t, filepath.Join(source, "types.yaml"), "scalars:\n  Label: text\n")
			writeReleaseFile(t, filepath.Join(source, "events.yaml"), "account.requested:\n  account_id: Label\n")
			writeReleaseFile(t, filepath.Join(source, "producer/events.yaml"), "account.ready:\n  key: account_id\n  account_id: Label\n")
			writeReleaseFile(t, filepath.Join(source, "account/entities.yaml"), "account_state:\n  account_id: {type: Label, _unused_reason: receiver instance identity}\n")
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config := filepath.Join(base, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, base, store.passwordEnv, 0)
			for _, command := range []string{"verify", "describe"} {
				args := []string{command, "--json", source, "--config", config}
				if command == "verify" {
					args = append(args, "--portable")
				}
				result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, args...)
				if result.err != nil {
					t.Fatalf("public template %s: %v\n%s", command, result.err, result.output)
				}
			}
			process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, Source: source, ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env})
			ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("public alias template boot: %v\n%s", err, process.output.String())
			}
			var published struct {
				RunID string `json:"run_id"`
			}
			if err := process.rpc.call(ctx, "event.publish", map[string]any{"bundle_hash": goldenServedBundleHash(t, process.rpc, "live"), "event_name": "account.requested", "payload": map[string]any{"account_id": "alias-key"}, "emitter": "releasee2e", "idempotency_key": "template-alias"}, &published); err != nil || published.RunID == "" {
				t.Fatalf("public template ingress: %+v %v", published, err)
			}
			var routed goldenEvent
			var observed []goldenEvent
			if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
				events, err := listGoldenEvents(ctx, process.rpc, published.RunID)
				if err != nil {
					return false, err
				}
				observed = events
				for _, event := range events {
					if event.EventName == "producer/account.ready" && len(event.Deliveries) == 1 && event.Deliveries[0].Status == "delivered" {
						routed = event
						return true, nil
					}
				}
				return false, nil
			}); err != nil {
				state, _ := json.Marshal(observed)
				t.Fatalf("alias template delivery: %v\nlast event state: %s\n%s", err, state, process.output.String())
			}
			target := routed.Deliveries[0].Target
			if target.FlowID != "account" || target.Kind != "materializing_entity" || !reflect.DeepEqual(routed.Payload, map[string]any{"account_id": "alias-key"}) {
				t.Fatalf("alias template route lost exact acquisition/payload: %+v", routed)
			}
			var entity map[string]any
			if err := process.rpc.call(ctx, "entity.get", map[string]any{"entity_id": target.EntityID, "run_id": published.RunID}, &entity); err != nil {
				t.Fatalf("alias template identity readback: %+v %v", entity, err)
			}
			summary, ok := entity["entity"].(map[string]any)
			if !ok || summary["flow_instance"] != target.FlowInstance || summary["run_id"] != published.RunID || !reflect.DeepEqual(entity["fields"], map[string]any{"account_id": "alias-key"}) {
				t.Fatalf("alias template identity readback: %+v", entity)
			}
			before, _ := json.Marshal(entity)
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			process = startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, Source: source, ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env})
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("alias template restart: %v\n%s", err, process.output.String())
			}
			if err := process.rpc.call(ctx, "entity.get", map[string]any{"entity_id": target.EntityID, "run_id": published.RunID}, &entity); err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(entity)
			if !bytes.Equal(before, after) {
				t.Fatalf("restart changed exact alias template authority: %s != %s", before, after)
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assertCatalogAdmissionJoin(t *testing.T, entity map[string]any, wantMembers []string) {
	t.Helper()
	accumulated, ok := entity["accumulated"].(map[string]any)
	if !ok {
		t.Fatalf("public join state is not an object: %#v", entity["accumulated"])
	}
	const nodeBucket = "handler_joins:Lg.Y29tcGxldGUtdGFzaw"
	joinBuckets := 0
	for key := range accumulated {
		if strings.HasPrefix(key, "handler_joins:") {
			joinBuckets++
		}
	}
	bucket, ok := accumulated[nodeBucket].(map[string]any)
	if !ok || joinBuckets != 1 {
		t.Fatalf("public join has missing or foreign node ownership: %#v", accumulated)
	}
	joins, ok := bucket["handler_joins"].(map[string]any)
	if !ok || len(joins) != 1 {
		t.Fatalf("public join did not retain exactly one activation: %#v", bucket)
	}
	for key, value := range joins {
		var join struct {
			Members        []string `json:"members"`
			MemberCount    *int     `json:"member_count"`
			Status         string   `json:"status"`
			CloseReason    string   `json:"close_reason"`
			OutcomeFired   bool     `json:"outcome_fired"`
			OutcomePending bool     `json:"outcome_pending"`
			Outputs        map[string]struct {
				Hash  string `json:"hash"`
				Value string `json:"value"`
			} `json:"outputs"`
		}
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &join); err != nil {
			t.Fatal(err)
		}
		if key == "" || !reflect.DeepEqual(join.Members, wantMembers) || join.MemberCount != nil || len(join.Outputs) != len(wantMembers) || join.Status != "closed" || join.CloseReason != "complete" || !join.OutcomeFired || join.OutcomePending {
			t.Fatalf("public join did not settle exact ordered membership: %s", raw)
		}
		for _, member := range wantMembers {
			output, ok := join.Outputs[member]
			if !ok || output.Hash == "" || output.Value != member {
				t.Fatalf("public join lost the exact output for member %q: %s", member, raw)
			}
		}
	}
}

package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	bundlecatalog "github.com/division-sh/swarm/internal/bundlecatalog"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimerunbundle "github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

type bundleCatalogAgentPageStore interface {
	ListBundleCatalogAgents(context.Context, string, bundlecatalog.AgentListOptions) (bundlecatalog.AgentsResult, error)
}

func TestBundleCatalogReadSurfaceListGetAgentsAndCursor(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	olderHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	newerHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	now := time.Unix(1700000000, 0).UTC()
	olderAgent := bundleCatalogIntentDefinition(t, "researcher", "", "agents.yaml#agents.researcher.intent", "older intent")
	olderAgent["role"] = "research"
	olderAgent["type"] = "managed"
	olderParsedJSON, err := json.Marshal(map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []map[string]any{olderAgent}})
	if err != nil {
		t.Fatal(err)
	}
	rootAgent := bundleCatalogIntentDefinition(t, "researcher", "", "agents.yaml#agents.researcher.intent", "  root intent\n")
	rootAgent["role"] = "research"
	rootAgent["type"] = "managed"
	rootAgent["model"] = "cheap"
	rootAgent["llm_backend"] = "claude"
	rootAgent["memory"] = false
	rootAgent["subscriptions"] = []string{"scan.requested"}
	rootAgent["tools"] = []string{"web_search"}
	flowAgent := bundleCatalogIntentDefinition(t, "researcher", "review/primary", "flows/review/agents.yaml#agents.researcher.intent", "flow intent")
	flowAgent["role"] = "review"
	flowAgent["type"] = "managed"
	flowAgent["memory"] = false
	newerParsedJSON, err := json.Marshal(map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []map[string]any{rootAgent, flowAgent}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, data_blob, metadata, ingested_at)
		VALUES
			($1, $2, $3::jsonb, NULL, $4::jsonb, $5),
			($6, $7, $8::jsonb, $9::bytea, $10::jsonb, $11)
	`, olderHash, `
agents:
  researcher:
    role: research
    type: managed
`, string(olderParsedJSON), `{"source":"older"}`, now.Add(-time.Hour),
		newerHash, `name: newer`, string(newerParsedJSON), []byte("blob"), `{"source":"newer"}`, now); err != nil {
		t.Fatalf("seed bundles: %v", err)
	}

	first, err := pg.ListBundleCatalog(ctx, bundlecatalog.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListBundleCatalog first: %v", err)
	}
	if len(first.Bundles) != 1 || first.Bundles[0].BundleHash != newerHash {
		t.Fatalf("first page = %#v, want newest bundle", first.Bundles)
	}
	if first.Bundles[0].AgentCount != 2 || !first.Bundles[0].HasData || first.Bundles[0].DataSizeBytes != 4 {
		t.Fatalf("newer summary = %#v", first.Bundles[0])
	}
	if first.NextCursor == "" {
		t.Fatal("first page cursor empty")
	}

	second, err := pg.ListBundleCatalog(ctx, bundlecatalog.ListOptions{Limit: 1, Cursor: first.NextCursor})
	if err != nil {
		t.Fatalf("ListBundleCatalog second: %v", err)
	}
	if len(second.Bundles) != 1 || second.Bundles[0].BundleHash != olderHash || second.NextCursor != "" {
		t.Fatalf("second page = %#v cursor=%q, want older only", second.Bundles, second.NextCursor)
	}
	if second.Bundles[0].AgentCount != 1 || second.Bundles[0].HasData {
		t.Fatalf("older summary = %#v", second.Bundles[0])
	}

	detail, err := pg.LoadBundleCatalog(ctx, newerHash)
	if err != nil {
		t.Fatalf("LoadBundleCatalog: %v", err)
	}
	if detail.BundleHash != newerHash || detail.Metadata["source"] != "newer" || detail.AgentCount != 2 || !detail.HasData {
		t.Fatalf("detail = %#v", detail)
	}

	agents, err := pg.ListBundleCatalogAgents(ctx, newerHash, bundlecatalog.AgentListOptions{})
	if err != nil {
		t.Fatalf("ListBundleCatalogAgents: %v", err)
	}
	if len(agents.Agents) != 2 {
		t.Fatalf("agents = %#v, want two definitions", agents.Agents)
	}
	if agents.Agents[0].AgentID != "researcher" || agents.Agents[0].FlowInstance != "" || agents.Agents[0].Model != "cheap" {
		t.Fatalf("root agent = %#v", agents.Agents[0])
	}
	if agents.Agents[1].AgentID != "researcher" || agents.Agents[1].FlowInstance != "review/primary" {
		t.Fatalf("flow agent = %#v", agents.Agents[1])
	}
	if agents.Agents[0].IntentContent != "  root intent\n" || agents.Agents[0].IntentContentHash == "" || agents.Agents[0].IntentIdentity == "" {
		t.Fatalf("root intent readback = %#v, want exact content and canonical identity", agents.Agents[0])
	}

	runtimeRecord, err := pg.LoadBundleCatalogRuntimeRecord(ctx, newerHash)
	if err != nil {
		t.Fatalf("LoadBundleCatalogRuntimeRecord: %v", err)
	}
	if runtimeRecord.BundleHash != newerHash || runtimeRecord.ContentYAML != `name: newer` || string(runtimeRecord.DataBlob) != "blob" {
		t.Fatalf("runtime record = %#v", runtimeRecord)
	}

	olderRuntimeRecord, err := pg.LoadBundleCatalogRuntimeRecord(ctx, olderHash)
	if err != nil {
		t.Fatalf("LoadBundleCatalogRuntimeRecord older: %v", err)
	}
	if olderRuntimeRecord.BundleHash != olderHash || olderRuntimeRecord.DataBlob != nil {
		t.Fatalf("older runtime record = %#v, want nil data blob", olderRuntimeRecord)
	}
}

func bundleCatalogIntentDefinition(t testing.TB, agentID, flowInstance, provenance, content string) map[string]any {
	t.Helper()
	intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", provenance, content)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"agent_id":            agentID,
		"agent_name_owner":    "swarm://" + strings.Trim(strings.ReplaceAll(provenance, "#agents.", "/agent/"), "/"),
		"flow_instance":       flowInstance,
		"memory":              false,
		"memory_source":       "authored",
		"intent_kind":         string(intent.Kind),
		"intent_source":       intent.Coordinate,
		"intent_provenance":   intent.Provenance,
		"intent_content_hash": intent.ContentHash,
		"intent_identity":     intent.Identity,
		"intent_content":      intent.Content,
	}
}

func TestBundleCatalogReadSurfaceMissingCursorAndMalformedProjection(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	missingHash := "bundle-v1:sha256:9999999999999999999999999999999999999999999999999999999999999999"
	if _, err := pg.LoadBundleCatalog(ctx, missingHash); !errors.Is(err, bundlecatalog.ErrNotFound) {
		t.Fatalf("LoadBundleCatalog missing error = %v, want bundlecatalog.ErrNotFound", err)
	}
	if _, err := pg.LoadBundleCatalogRuntimeRecord(ctx, missingHash); !errors.Is(err, runtimerunbundle.ErrBundleNotFound) {
		t.Fatalf("LoadBundleCatalogRuntimeRecord missing error = %v, want runbundle.ErrBundleNotFound", err)
	}
	if _, err := pg.ListBundleCatalog(ctx, bundlecatalog.ListOptions{Cursor: "not-a-cursor"}); !errors.Is(err, bundlecatalog.ErrInvalidCursor) {
		t.Fatalf("ListBundleCatalog invalid cursor error = %v, want bundlecatalog.ErrInvalidCursor", err)
	}

	badHash := "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: bad', $2::jsonb)
	`, badHash, `{"projection_version":"swarm.bundle.catalog.v2","agents":[{"agent_id":"bad","agent_name_owner":"swarm://agent/bad","status":"running"}]}`); err != nil {
		t.Fatalf("seed malformed bundle: %v", err)
	}
	_, err := pg.ListBundleCatalogAgents(ctx, badHash, bundlecatalog.AgentListOptions{})
	if err == nil || !strings.Contains(err.Error(), "runtime field") {
		t.Fatalf("ListBundleCatalogAgents malformed error = %v, want runtime field rejection", err)
	}
}

func TestBundleCatalogAgentPagesMatchAcrossPostgresAndSQLite(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) (bundleCatalogAgentPageStore, *sql.DB, bool)
	}{
		{
			name: "postgres",
			open: func(t *testing.T) (bundleCatalogAgentPageStore, *sql.DB, bool) {
				_, db, _ := testutil.StartPostgres(t)
				store := admitTestPostgresStore(t, db)
				return store, store.backend.ConstructionHandle(), true
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T) (bundleCatalogAgentPageStore, *sql.DB, bool) {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				return store, store.backend.ConstructionHandle(), false
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			store, db, postgres := backend.open(t)
			ctx := testAuthorActivityContext()

			bundleHash := "bundle-v1:sha256:" + strings.Repeat("a", 64)
			definitions := []map[string]any{
				bundleCatalogAgentWithOwner(t, "worker", "flows/review", "swarm://flows/review/agent/worker", "flow intent"),
				bundleCatalogAgentWithOwner(t, "worker", "flows/triage", "swarm://flows/triage/agent/worker", "flow intent"),
				bundleCatalogAgentWithOwner(t, "worker", "", "swarm://projects/right/agent/worker", "right intent"),
				bundleCatalogAgentWithOwner(t, "worker", "", "swarm://agent/worker", "root intent"),
				bundleCatalogAgentWithOwner(t, "worker", "", "swarm://projects/left/agent/worker", "left intent"),
			}
			seedBundleCatalogAgentProjection(t, ctx, db, postgres, bundleHash, definitions)

			var owners []string
			cursor := ""
			for pageNumber := 0; ; pageNumber++ {
				page, err := store.ListBundleCatalogAgents(ctx, bundleHash, bundlecatalog.AgentListOptions{Limit: 1, Cursor: cursor})
				if err != nil {
					t.Fatalf("page %d: %v", pageNumber, err)
				}
				if len(page.Agents) != 1 {
					t.Fatalf("page %d agents = %d, want 1", pageNumber, len(page.Agents))
				}
				owners = append(owners, page.Agents[0].AgentNameOwner)
				if page.NextCursor == "" {
					break
				}
				if page.NextCursor == cursor {
					t.Fatalf("page %d cursor did not advance", pageNumber)
				}
				cursor = page.NextCursor
			}
			wantOwners := []string{
				"swarm://agent/worker",
				"swarm://flows/review/agent/worker",
				"swarm://flows/triage/agent/worker",
				"swarm://projects/left/agent/worker",
				"swarm://projects/right/agent/worker",
			}
			if fmt.Sprint(owners) != fmt.Sprint(wantOwners) {
				t.Fatalf("owners = %#v, want %#v", owners, wantOwners)
			}

			for name, invalidCursor := range map[string]string{
				"malformed":    "not-base64",
				"legacy":       base64.RawURLEncoding.EncodeToString([]byte(`{"agent_id":"worker"}`)),
				"cross_bundle": testBundleCatalogAgentCursor("bundle-v1:sha256:"+strings.Repeat("b", 64), wantOwners[0]),
				"unknown":      testBundleCatalogAgentCursor(bundleHash, "swarm://agent/unknown"),
			} {
				t.Run(name, func(t *testing.T) {
					result, err := store.ListBundleCatalogAgents(ctx, bundleHash, bundlecatalog.AgentListOptions{Cursor: invalidCursor})
					if !errors.Is(err, bundlecatalog.ErrInvalidCursor) || len(result.Agents) != 0 {
						t.Fatalf("result=%#v err=%v, want fail-closed invalid cursor", result, err)
					}
				})
			}

			valid := bundleCatalogAgentWithOwner(t, "worker", "", "swarm://agent/worker", "intent")
			missingOwner := cloneBundleCatalogAgentDefinition(valid)
			delete(missingOwner, "agent_name_owner")
			for index, malformed := range []map[string]any{
				{"projection_version": "swarm.bundle.catalog.v1", "agents": []any{}},
				{"projection_version": "swarm.bundle.catalog.v2", "agents": []any{missingOwner}},
				{"projection_version": "swarm.bundle.catalog.v2", "agents": []any{valid, valid}},
			} {
				malformedHash := fmt.Sprintf("bundle-v1:sha256:%064x", index+1)
				seedRawBundleCatalogProjection(t, ctx, db, postgres, malformedHash, malformed)
				result, err := store.ListBundleCatalogAgents(ctx, malformedHash, bundlecatalog.AgentListOptions{})
				if err == nil || len(result.Agents) != 0 {
					t.Fatalf("malformed projection %d result=%#v err=%v, want rejection before rows", index, result, err)
				}
			}

			countHash := "bundle-v1:sha256:" + strings.Repeat("c", 64)
			many := make([]map[string]any, 0, bundlecatalog.MaxAgentListLimit+1)
			for index := 0; index <= bundlecatalog.MaxAgentListLimit; index++ {
				many = append(many, bundleCatalogAgentWithOwner(t, "worker", "", fmt.Sprintf("swarm://agent/%04d", index), "intent"))
			}
			seedBundleCatalogAgentProjection(t, ctx, db, postgres, countHash, many)
			defaultPage, err := store.ListBundleCatalogAgents(ctx, countHash, bundlecatalog.AgentListOptions{})
			if err != nil || len(defaultPage.Agents) != bundlecatalog.DefaultAgentListLimit || defaultPage.NextCursor == "" {
				t.Fatalf("default page agents=%d cursor=%q err=%v", len(defaultPage.Agents), defaultPage.NextCursor, err)
			}
			maxPage, err := store.ListBundleCatalogAgents(ctx, countHash, bundlecatalog.AgentListOptions{Limit: bundlecatalog.MaxAgentListLimit})
			if err != nil || len(maxPage.Agents) != bundlecatalog.MaxAgentListLimit || maxPage.NextCursor == "" {
				t.Fatalf("max page agents=%d cursor=%q err=%v", len(maxPage.Agents), maxPage.NextCursor, err)
			}

			largeHash := "bundle-v1:sha256:" + strings.Repeat("d", 64)
			large := []map[string]any{
				bundleCatalogAgentWithOwner(t, "first", "", "swarm://agent/first", strings.Repeat("a", 500_000)),
				bundleCatalogAgentWithOwner(t, "second", "", "swarm://agent/second", strings.Repeat("b", 500_000)),
			}
			seedBundleCatalogAgentProjection(t, ctx, db, postgres, largeHash, large)
			first, err := store.ListBundleCatalogAgents(ctx, largeHash, bundlecatalog.AgentListOptions{Limit: 2})
			encoded, marshalErr := json.Marshal(first)
			if err != nil || marshalErr != nil || len(first.Agents) != 1 || first.NextCursor == "" || len(encoded) > bundlecatalog.AgentListResultByteCeiling {
				t.Fatalf("byte page agents=%d cursor=%q bytes=%d err=%v marshal=%v", len(first.Agents), first.NextCursor, len(encoded), err, marshalErr)
			}
			second, err := store.ListBundleCatalogAgents(ctx, largeHash, bundlecatalog.AgentListOptions{Limit: 2, Cursor: first.NextCursor})
			if err != nil || len(second.Agents) != 1 || second.Agents[0].AgentID != "second" || second.NextCursor != "" {
				t.Fatalf("second byte page=%#v err=%v", second, err)
			}

			oversizedHash := "bundle-v1:sha256:" + strings.Repeat("e", 64)
			oversized := []map[string]any{
				bundleCatalogAgentWithOwner(t, "oversized", "", "swarm://agent/oversized", strings.Repeat("x", bundlecatalog.AgentListResultByteCeiling)),
			}
			seedBundleCatalogAgentProjection(t, ctx, db, postgres, oversizedHash, oversized)
			result, err := store.ListBundleCatalogAgents(ctx, oversizedHash, bundlecatalog.AgentListOptions{})
			var tooLarge *bundlecatalog.AgentDefinitionTooLargeError
			if !errors.As(err, &tooLarge) || len(result.Agents) != 0 || tooLarge.AgentNameOwner != "swarm://agent/oversized" {
				t.Fatalf("oversized result=%#v err=%v details=%#v", result, err, tooLarge)
			}
		})
	}
}

func bundleCatalogAgentWithOwner(t testing.TB, agentID, flowInstance, owner, content string) map[string]any {
	t.Helper()
	definition := bundleCatalogIntentDefinition(t, agentID, flowInstance, "agents.yaml#agents."+agentID+".intent", content)
	definition["agent_name_owner"] = owner
	return definition
}

func cloneBundleCatalogAgentDefinition(definition map[string]any) map[string]any {
	clone := make(map[string]any, len(definition))
	for key, value := range definition {
		clone[key] = value
	}
	return clone
}

func seedBundleCatalogAgentProjection(t testing.TB, ctx context.Context, db *sql.DB, postgres bool, bundleHash string, definitions []map[string]any) {
	t.Helper()
	items := make([]any, 0, len(definitions))
	for _, definition := range definitions {
		items = append(items, definition)
	}
	seedRawBundleCatalogProjection(t, ctx, db, postgres, bundleHash, map[string]any{
		"projection_version": "swarm.bundle.catalog.v2",
		"agents":             items,
	})
}

func seedRawBundleCatalogProjection(t testing.TB, ctx context.Context, db *sql.DB, postgres bool, bundleHash string, projection map[string]any) {
	t.Helper()
	raw, err := json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	var query string
	if postgres {
		query = `INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, metadata) VALUES ($1, '', $2::jsonb, '{}'::jsonb)`
	} else {
		query = `INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, metadata) VALUES (?, '', ?, '{}')`
	}
	if _, err := db.ExecContext(ctx, query, bundleHash, string(raw)); err != nil {
		t.Fatalf("seed malformed bundle projection: %v", err)
	}
}

func testBundleCatalogAgentCursor(bundleHash, owner string) string {
	raw, _ := json.Marshal(map[string]any{
		"version":          "swarm.bundle.agents.cursor.v1",
		"bundle_hash":      bundleHash,
		"agent_name_owner": owner,
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func TestBundleCatalogWriteSurfaceUpsertsAndRejectsHashCollision(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	bundleHash := "bundle-v1:sha256:5555555555555555555555555555555555555555555555555555555555555555"
	agent := bundleCatalogIntentDefinition(t, "researcher", "", "agents.yaml#agents.researcher.intent", "Research the requested subject.")
	agent["role"] = "research"
	req := bundlecatalog.Upsert{
		BundleHash:  bundleHash,
		ContentYAML: "projection_version: swarm.bundle.catalog.v2\nfiles: []\n",
		ParsedJSON:  map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []map[string]any{agent}},
		DataBlob:    []byte(`{"projection_version":"swarm.bundle.catalog.v2","entries":[]}`),
		Metadata: map[string]any{
			"source": "test",
		},
	}

	first, err := pg.UpsertBundleCatalog(ctx, req)
	if err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}
	detail := first.Detail
	if detail.BundleHash != bundleHash || detail.AgentCount != 1 || !detail.HasData || detail.Metadata["source"] != "test" {
		t.Fatalf("detail = %#v", detail)
	}
	if !first.Registered {
		t.Fatalf("first upsert registered = false, want true")
	}

	duplicate, err := pg.UpsertBundleCatalog(ctx, req)
	if err != nil {
		t.Fatalf("UpsertBundleCatalog idempotent: %v", err)
	}
	if duplicate.Registered {
		t.Fatalf("duplicate upsert registered = true, want false")
	}
	req.ContentYAML = "projection_version: swarm.bundle.catalog.v2\nfiles: [changed]\n"
	if _, err := pg.UpsertBundleCatalog(ctx, req); !errors.Is(err, bundlecatalog.ErrConflict) {
		t.Fatalf("UpsertBundleCatalog collision error = %v, want bundlecatalog.ErrConflict", err)
	}
}

func TestBundleCatalogUpsertRegistersDuplicatesConflictsAndDoesNotRestoreDeletedRuns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	bundleHash := "bundle-v1:sha256:4444444444444444444444444444444444444444444444444444444444444444"
	req := bundlecatalog.Upsert{
		BundleHash:  bundleHash,
		ContentYAML: "projection_version: swarm.bundle.catalog.v2\nfiles: []\ncanonical_inputs: []\n",
		ParsedJSON:  map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []any{}},
		DataBlob:    []byte(`{"entries":[]}`),
		Metadata:    map[string]any{"source": "bundle.register"},
	}
	first, err := pg.UpsertBundleCatalog(ctx, req)
	if err != nil {
		t.Fatalf("UpsertBundleCatalog first: %v", err)
	}
	if !first.Registered || first.Detail.BundleHash != bundleHash || !first.Detail.HasData {
		t.Fatalf("first upsert = %#v", first)
	}

	metadataConflict := req
	metadataConflict.Metadata = map[string]any{"source": "swarm serve --contracts"}
	if _, err := pg.UpsertBundleCatalog(ctx, metadataConflict); !errors.Is(err, bundlecatalog.ErrConflict) {
		t.Fatalf("UpsertBundleCatalog metadata conflict error = %v, want bundlecatalog.ErrConflict", err)
	}

	duplicate, err := pg.UpsertBundleCatalog(ctx, req)
	if err != nil {
		t.Fatalf("UpsertBundleCatalog duplicate: %v", err)
	}
	if duplicate.Registered || duplicate.Detail.Metadata["source"] != "bundle.register" {
		t.Fatalf("duplicate upsert = %#v, want no-op preserving original row", duplicate)
	}

	conflict := req
	conflict.ContentYAML = "projection_version: swarm.bundle.catalog.v2\nfiles:\n  - label: \"bundle/package.yaml\"\n    content_base64: \"e30=\"\n    size_bytes: 2\ncanonical_inputs: []\n"
	if _, err := pg.UpsertBundleCatalog(ctx, conflict); !errors.Is(err, bundlecatalog.ErrConflict) {
		t.Fatalf("UpsertBundleCatalog conflict error = %v, want bundlecatalog.ErrConflict", err)
	}

	runID := "00000000-0000-0000-0000-000000000444"
	runlifecyclefixture.RequireCorruptPostgresSnapshot(t, ctx, db, runlifecyclefixture.CorruptSnapshot{OriginKind: runlifecyclefixture.ScenarioSetupOriginKind(),
		RunID: runID, State: "completed", BundleHash: bundleHash, BundleSource: "deleted",
	})
	if _, err := pg.UpsertBundleCatalog(ctx, req); err != nil {
		t.Fatalf("UpsertBundleCatalog deleted re-register: %v", err)
	}
	var source string
	if err := db.QueryRowContext(ctx, `SELECT bundle_source FROM runs WHERE run_id = $1::uuid`, runID).Scan(&source); err != nil {
		t.Fatalf("load run bundle_source: %v", err)
	}
	if source != "deleted" {
		t.Fatalf("bundle_source after re-register = %q, want deleted", source)
	}
}

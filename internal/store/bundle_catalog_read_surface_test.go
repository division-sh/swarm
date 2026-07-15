package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestBundleCatalogReadSurfaceListGetAgentsAndCursor(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := testAuthorActivityContext()

	olderHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	newerHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	now := time.Unix(1700000000, 0).UTC()
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
`, `{}`, `{"source":"older"}`, now.Add(-time.Hour),
		newerHash, `name: newer`, `{
			"agents": {
				"researcher": {
					"role": "research",
					"type": "managed",
					"model": "cheap",
					"llm_backend": "claude",
					"memory": false,
					"prompt_path": "prompts/researcher.md",
					"subscriptions": ["scan.requested"],
					"tools": ["web_search"]
				}
			},
			"flows": {
				"review/primary": {
					"agents": {
						"reviewer": {
							"role": "review",
							"type": "managed"
						}
					}
				}
			}
		}`, []byte("blob"), `{"source":"newer"}`, now); err != nil {
		t.Fatalf("seed bundles: %v", err)
	}

	first, err := pg.ListBundleCatalog(ctx, BundleCatalogListOptions{Limit: 1})
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

	second, err := pg.ListBundleCatalog(ctx, BundleCatalogListOptions{Limit: 1, Cursor: first.NextCursor})
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

	agents, err := pg.ListBundleCatalogAgents(ctx, newerHash)
	if err != nil {
		t.Fatalf("ListBundleCatalogAgents: %v", err)
	}
	if len(agents.Agents) != 2 {
		t.Fatalf("agents = %#v, want two definitions", agents.Agents)
	}
	if agents.Agents[0].AgentID != "researcher" || agents.Agents[0].FlowInstance != "" || agents.Agents[0].Model != "cheap" {
		t.Fatalf("root agent = %#v", agents.Agents[0])
	}
	if agents.Agents[1].AgentID != "reviewer" || agents.Agents[1].FlowInstance != "review/primary" {
		t.Fatalf("flow agent = %#v", agents.Agents[1])
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

func TestBundleCatalogReadSurfaceMissingCursorAndMalformedProjection(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := testAuthorActivityContext()

	missingHash := "bundle-v1:sha256:9999999999999999999999999999999999999999999999999999999999999999"
	if _, err := pg.LoadBundleCatalog(ctx, missingHash); !errors.Is(err, ErrBundleNotFound) {
		t.Fatalf("LoadBundleCatalog missing error = %v, want ErrBundleNotFound", err)
	}
	if _, err := pg.LoadBundleCatalogRuntimeRecord(ctx, missingHash); !errors.Is(err, ErrBundleNotFound) {
		t.Fatalf("LoadBundleCatalogRuntimeRecord missing error = %v, want ErrBundleNotFound", err)
	}
	if _, err := pg.ListBundleCatalog(ctx, BundleCatalogListOptions{Cursor: "not-a-cursor"}); !errors.Is(err, ErrInvalidBundleCatalogCursor) {
		t.Fatalf("ListBundleCatalog invalid cursor error = %v, want ErrInvalidBundleCatalogCursor", err)
	}

	badHash := "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: bad', $2::jsonb)
	`, badHash, `{"agents":{"bad":{"status":"running"}}}`); err != nil {
		t.Fatalf("seed malformed bundle: %v", err)
	}
	_, err := pg.ListBundleCatalogAgents(ctx, badHash)
	if err == nil || !strings.Contains(err.Error(), "runtime field") {
		t.Fatalf("ListBundleCatalogAgents malformed error = %v, want runtime field rejection", err)
	}
}

func TestBundleCatalogWriteSurfaceUpsertsAndRejectsHashCollision(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := testAuthorActivityContext()
	bundleHash := "bundle-v1:sha256:5555555555555555555555555555555555555555555555555555555555555555"
	req := BundleCatalogUpsert{
		BundleHash:  bundleHash,
		ContentYAML: "projection_version: swarm.bundle.catalog.v1\nfiles: []\n",
		ParsedJSON: map[string]any{
			"agents": map[string]any{
				"researcher": map[string]any{
					"role": "research",
				},
			},
		},
		DataBlob: []byte(`{"projection_version":"swarm.bundle.catalog.v1","entries":[]}`),
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
	req.ContentYAML = "projection_version: swarm.bundle.catalog.v1\nfiles: [changed]\n"
	if _, err := pg.UpsertBundleCatalog(ctx, req); !errors.Is(err, ErrBundleCatalogConflict) {
		t.Fatalf("UpsertBundleCatalog collision error = %v, want ErrBundleCatalogConflict", err)
	}
}

func TestBundleCatalogUpsertRegistersDuplicatesConflictsAndDoesNotRestoreDeletedRuns(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := testAuthorActivityContext()

	bundleHash := "bundle-v1:sha256:4444444444444444444444444444444444444444444444444444444444444444"
	req := BundleCatalogUpsert{
		BundleHash:  bundleHash,
		ContentYAML: "projection_version: swarm.bundle.catalog.v1\nfiles: []\ncanonical_inputs: []\n",
		ParsedJSON:  map[string]any{"agents": map[string]any{}},
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
	if _, err := pg.UpsertBundleCatalog(ctx, metadataConflict); !errors.Is(err, ErrBundleCatalogConflict) {
		t.Fatalf("UpsertBundleCatalog metadata conflict error = %v, want ErrBundleCatalogConflict", err)
	}

	duplicate, err := pg.UpsertBundleCatalog(ctx, req)
	if err != nil {
		t.Fatalf("UpsertBundleCatalog duplicate: %v", err)
	}
	if duplicate.Registered || duplicate.Detail.Metadata["source"] != "bundle.register" {
		t.Fatalf("duplicate upsert = %#v, want no-op preserving original row", duplicate)
	}

	conflict := req
	conflict.ContentYAML = "projection_version: swarm.bundle.catalog.v1\nfiles:\n  - label: \"bundle/package.yaml\"\n    content_base64: \"e30=\"\n    size_bytes: 2\ncanonical_inputs: []\n"
	if _, err := pg.UpsertBundleCatalog(ctx, conflict); !errors.Is(err, ErrBundleCatalogConflict) {
		t.Fatalf("UpsertBundleCatalog conflict error = %v, want ErrBundleCatalogConflict", err)
	}

	runID := "00000000-0000-0000-0000-000000000444"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES ($1::uuid, 'completed', $2, 'deleted', NOW())
	`, runID, bundleHash); err != nil {
		t.Fatalf("seed deleted run: %v", err)
	}
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

package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"swarm/internal/testutil"
)

func TestBundleCatalogReadSurfaceListGetAgentsAndCursor(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

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
					"model_tier": "haiku",
					"llm_backend": "claude",
					"conversation_mode": "session",
					"session_scope": "flow",
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
	if agents.Agents[0].AgentID != "researcher" || agents.Agents[0].FlowInstance != "" || agents.Agents[0].ModelTier != "haiku" {
		t.Fatalf("root agent = %#v", agents.Agents[0])
	}
	if agents.Agents[1].AgentID != "reviewer" || agents.Agents[1].FlowInstance != "review/primary" {
		t.Fatalf("flow agent = %#v", agents.Agents[1])
	}
}

func TestBundleCatalogReadSurfaceMissingCursorAndMalformedProjection(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	missingHash := "bundle-v1:sha256:9999999999999999999999999999999999999999999999999999999999999999"
	if _, err := pg.LoadBundleCatalog(ctx, missingHash); !errors.Is(err, ErrBundleNotFound) {
		t.Fatalf("LoadBundleCatalog missing error = %v, want ErrBundleNotFound", err)
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
	ctx := context.Background()
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

	detail, err := pg.UpsertBundleCatalog(ctx, req)
	if err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}
	if detail.BundleHash != bundleHash || detail.AgentCount != 1 || !detail.HasData || detail.Metadata["source"] != "test" {
		t.Fatalf("detail = %#v", detail)
	}

	if _, err := pg.UpsertBundleCatalog(ctx, req); err != nil {
		t.Fatalf("UpsertBundleCatalog idempotent: %v", err)
	}
	req.ContentYAML = "projection_version: swarm.bundle.catalog.v1\nfiles: [changed]\n"
	if _, err := pg.UpsertBundleCatalog(ctx, req); err == nil || !strings.Contains(err.Error(), "different content") {
		t.Fatalf("UpsertBundleCatalog collision error = %v, want different content", err)
	}
}

package cliapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
)

func TestStreamDataJSONLAcceptsOneBasedMultiPageExport(t *testing.T) {
	rows := [][]byte{[]byte("{\"slug\":\"alpha\"}\n"), []byte("{\"slug\":\"beta\"}\n")}
	versionID := "resource-version-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	contentDigest := "resource-content-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		page, _ := request.Params["page"].(map[string]any)
		if requests == 0 {
			if _, found := page["cursor"]; found {
				t.Errorf("first page unexpectedly has cursor: %#v", page)
			}
		} else if page["cursor"] != "next" {
			t.Errorf("second page cursor = %#v", page["cursor"])
		}
		row := rows[requests]
		continuation := map[string]any{"state": "end"}
		if requests == 0 {
			continuation = map[string]any{"state": "more", "cursor": "next"}
		}
		result := map[string]any{
			"declaration": map[string]any{"package_key": ".", "event": "startup.loaded"},
			"version_id":  versionID, "content_digest": contentDigest, "total_rows": 2,
			"first_ordinal": requests + 1, "row_count": 1,
			"chunk_base64": base64.StdEncoding.EncodeToString(row), "chunk_bytes": len(row),
			"chunk_sha256": durableDataTestSHA256(row), "continuation": continuation,
		}
		requests++
		writeJSONRPCResult(t, w, request.ID, result)
	}))
	defer server.Close()

	client := &cliAPIClient{endpoint: server.URL, token: "test-token", httpClient: server.Client()}
	ref, err := durabledata.ParseDeclarationRef(".", "startup.loaded")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := streamDataJSONL(context.Background(), client, &output, ref, map[string]any{"kind": "head"}); err != nil {
		t.Fatal(err)
	}
	if got, want := output.String(), string(rows[0])+string(rows[1]); got != want || requests != 2 {
		t.Fatalf("streamed export = %q over %d requests, want %q over 2", got, requests, want)
	}
}

func TestStreamDataJSONLAcceptsCanonicalEmptyExport(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		requests++
		writeJSONRPCResult(t, w, request.ID, map[string]any{
			"declaration":    map[string]any{"package_key": ".", "event": "startup.loaded"},
			"version_id":     "resource-version-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"content_digest": "resource-content-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"total_rows":     0, "first_ordinal": 0, "row_count": 0,
			"chunk_base64": "", "chunk_bytes": 0, "chunk_sha256": durableDataTestSHA256(nil),
			"continuation": map[string]any{"state": "end"},
		})
	}))
	defer server.Close()

	client := &cliAPIClient{endpoint: server.URL, token: "test-token", httpClient: server.Client()}
	ref, err := durabledata.ParseDeclarationRef(".", "startup.loaded")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := streamDataJSONL(context.Background(), client, &output, ref, map[string]any{"kind": "head"}); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 || requests != 1 {
		t.Fatalf("empty export wrote %d bytes over %d requests", output.Len(), requests)
	}
}

func TestStreamDataJSONLRejectsContradictoryEmptyExportShapes(t *testing.T) {
	for _, test := range []struct {
		name         string
		totalRows    int
		firstOrdinal int
		rowCount     int
		content      []byte
		continuation map[string]any
	}{
		{name: "legacy one-based empty", firstOrdinal: 1, continuation: map[string]any{"state": "end"}},
		{name: "zero ordinal with rows", totalRows: 1, rowCount: 1, content: []byte("{\"slug\":\"alpha\"}\n"), continuation: map[string]any{"state": "end"}},
		{name: "empty continuation", firstOrdinal: 0, continuation: map[string]any{"state": "more", "cursor": "next"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Errorf("decode request: %v", err)
					return
				}
				writeJSONRPCResult(t, w, request.ID, map[string]any{
					"declaration":    map[string]any{"package_key": ".", "event": "startup.loaded"},
					"version_id":     "resource-version-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"content_digest": "resource-content-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"total_rows":     test.totalRows, "first_ordinal": test.firstOrdinal, "row_count": test.rowCount,
					"chunk_base64": base64.StdEncoding.EncodeToString(test.content), "chunk_bytes": len(test.content),
					"chunk_sha256": durableDataTestSHA256(test.content), "continuation": test.continuation,
				})
			}))
			defer server.Close()
			client := &cliAPIClient{endpoint: server.URL, token: "test-token", httpClient: server.Client()}
			ref, err := durabledata.ParseDeclarationRef(".", "startup.loaded")
			if err != nil {
				t.Fatal(err)
			}
			if err := streamDataJSONL(context.Background(), client, &bytes.Buffer{}, ref, map[string]any{"kind": "head"}); err == nil {
				t.Fatal("streamDataJSONL accepted contradictory empty export")
			}
		})
	}
}

func TestDataVersionSelectorUsesCanonicalAliasOwner(t *testing.T) {
	for _, alias := range []string{"v0", "v01", "v+1", "V1", "v1 "} {
		t.Run(alias, func(t *testing.T) {
			if _, err := dataVersionSelector(alias, true); err == nil {
				t.Fatalf("dataVersionSelector accepted noncanonical alias %q", alias)
			}
		})
	}
	selector, err := dataVersionSelector("v1", true)
	if err != nil || selector["kind"] != "alias" || selector["alias"] != "v1" {
		t.Fatalf("canonical alias selector = %#v, %v", selector, err)
	}
}

func durableDataTestSHA256(payload []byte) string {
	hash := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(hash[:])
}

package serveapp

import (
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestServedArtifactPublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"root", "static"} {
			for _, outcome := range []string{"success", "failure"} {
				t.Run(backend+"/"+mode+"/"+outcome, func(t *testing.T) {
					artifactRoot := t.TempDir()
					t.Setenv("SWARM_ARTIFACT_ROOT", artifactRoot)
					opts, start := lifecycleRestartHarness(t, backend, canonicalrouting.CopyPublicationArtifact(t, mode))
					first, rt := start()
					name, local, kind, document := "artifact.requested", "commit.ok", "ready", "name: exact-content\n"
					flow := "."
					if mode == "static" {
						name, flow = "source/"+name, "source"
					}
					if outcome == "failure" {
						local, kind, document = "commit.failed", "failed", "wrong: missing-name\n"
					}
					repoID, requestID := uuid.NewString(), uuid.NewString()
					params := map[string]any{"event_name": name, "bundle_hash": rt.BundleHash, "idempotency_key": "artifact-seed",
						"payload": map[string]any{"repo_id": repoID, "request_id": requestID, "document": document}}
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
					eventName := local
					if flow != "." {
						eventName = flow + "/" + local
					}
					var eventID string
					for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
						err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2`, seed.RunID, eventName).Scan(&eventID)
						if err == nil {
							break
						}
						if err != sql.ErrNoRows {
							t.Fatal(err)
						}
						if outcome == "success" {
							failureName := "commit.failed"
							if flow != "." {
								failureName = flow + "/" + failureName
							}
							var failed string
							err := rt.DB.QueryRow(`SELECT CAST(payload AS TEXT) FROM events WHERE run_id=$1 AND event_name=$2`, seed.RunID, failureName).Scan(&failed)
							if err == nil {
								t.Fatalf("successful artifact request produced failure: %s", failed)
							}
							if err != sql.ErrNoRows {
								t.Fatal(err)
							}
						}
						time.Sleep(20 * time.Millisecond)
					}
					if eventID == "" {
						t.Fatalf("missing artifact %s\n%s\n%s", outcome, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID), first.outputString())
					}
					readback := func() string {
						waitPublicationSiteCompletion(t, rt, seed.RunID)
						var public operatorread.OperatorEventFull
						requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &public)
						if public.EventName != eventName || public.RunID != seed.RunID || public.Payload["result_kind"] != kind || public.Payload["request_id"] != requestID || public.Payload["source_event_id"] != seed.EventID {
							t.Fatalf("artifact readback lost occurrence identity: %+v", public)
						}
						var recipients []string
						for _, delivery := range public.Deliveries {
							if delivery.Status != "delivered" {
								t.Fatalf("artifact receiver did not execute: %+v", delivery)
							}
							recipients = append(recipients, delivery.SubscriberID)
						}
						var want []string
						for _, receiver := range []string{flow, "sink"} {
							node, err := identity.ParseExecutableNode(receiver, "local")
							if err != nil {
								t.Fatal(err)
							}
							want = append(want, node.Key())
						}
						sort.Strings(want)
						sort.Strings(recipients)
						if !reflect.DeepEqual(recipients, want) {
							t.Fatalf("artifact recipients=%v want=%v", recipients, want)
						}
						var rawSource, sourceKind string
						if err := rt.DB.QueryRow(`SELECT CAST(source_route AS TEXT), routing_source_kind FROM events WHERE event_id=$1`, eventID).Scan(&rawSource, &sourceKind); err != nil {
							t.Fatal(err)
						}
						var route events.RouteIdentity
						if err := json.Unmarshal([]byte(rawSource), &route); err != nil {
							t.Fatal(err)
						}
						validSource := sourceKind == "static_flow" && route.FlowID == flow && route.FlowInstance == flow
						if flow == "." {
							validSource = sourceKind == "root" && route.FlowID == "" && route.FlowInstance == ""
						}
						if !validSource {
							t.Fatalf("artifact source=%s %+v", sourceKind, route)
						}
						if route.EntityID == "" {
							t.Fatal("artifact action lost its authored receiving entity")
						}
						var rawFields string
						if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, seed.RunID, route.EntityID).Scan(&rawFields); err != nil {
							t.Fatal(err)
						}
						var fields map[string]any
						if err := json.Unmarshal([]byte(rawFields), &fields); err != nil {
							t.Fatal(err)
						}
						if fields["last_request_id"] != requestID || fields["last_source_event_id"] != seed.EventID {
							t.Fatalf("artifact state lost request/source evidence: %s", rawFields)
						}
						if outcome == "success" {
							if fields["status"] != "committed" || !reflect.DeepEqual(fields["failure"], map[string]any{}) {
								t.Fatalf("artifact state did not commit success: %s", rawFields)
							}
							for _, field := range []string{"repo_url", "current_ref", "file_manifest"} {
								if !reflect.DeepEqual(fields[field], public.Payload[field]) {
									t.Fatalf("artifact output %s differs from publication: %s", field, rawFields)
								}
							}
						} else if fields["status"] != "failed" || !reflect.DeepEqual(fields["failure"], public.Payload["failure"]) {
							t.Fatalf("artifact failure state differs from publication: %s", rawFields)
						}
						payload, err := json.Marshal(public.Payload)
						if err != nil {
							t.Fatal(err)
						}
						return string(payload)
					}
					before := readback()
					repoPath := filepath.Join(artifactRoot, "repos", "publication-proof", repoID+".git")
					checkArtifact := func() {
						t.Helper()
						if outcome != "success" {
							if _, err := os.Stat(repoPath); !os.IsNotExist(err) {
								t.Fatalf("schema-invalid artifact reached provider: %v", err)
							}
							return
						}
						content, err := os.ReadFile(filepath.Join(repoPath, "document.yaml"))
						if err != nil || string(content) != document {
							t.Fatalf("artifact bytes=%q err=%v", content, err)
						}
						// The provider creates an empty initialization commit. Count the
						// exact business request, not that unrelated repository setup.
						count, err := exec.Command("git", "-C", repoPath, "rev-list", "--count", "--fixed-strings", "--grep=Swarm-Request-Id: "+requestID, "HEAD").CombinedOutput()
						if err != nil || strings.TrimSpace(string(count)) != "1" {
							t.Fatalf("artifact commits=%q err=%v", count, err)
						}
					}
					checkArtifact()
					if code := first.stop(); code != 0 {
						t.Fatalf("artifact completed stop=%d", code)
					}
					setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
					_, rt = start()
					duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
					if duplicate.EventID != seed.EventID || duplicate.RunID != seed.RunID || readback() != before {
						t.Fatal("restart/duplicate changed artifact occurrence")
					}
					checkArtifact()
				})
			}
		}
	}
}

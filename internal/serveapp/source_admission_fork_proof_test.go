package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func Test2376VerifyServeArtifactParityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyLifecycleForkSource(t, true)
			opts, start := lifecycleRestartHarness(t, backend, root)
			var out, errOut bytes.Buffer
			if code := executeCLIFrom(context.Background(), root, []string{"verify", ".", "--json", "--config", opts.ConfigPath}, &out, &errOut, nil); code != 0 {
				t.Fatalf("verify code=%d: %s / %s", code, &out, &errOut)
			}
			var verified struct {
				Hash    string                          `json:"bundle_hash"`
				Members []sourceartifact.MemberEvidence `json:"members"`
			}
			if err := json.Unmarshal(out.Bytes(), &verified); err != nil {
				t.Fatal(err)
			}
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			// Publication must consume source that is already durably available.
			var durableBeforeConstruction bool
			opts.TestAPIListenerBound = func(_ net.Addr) {
				if selected == nil {
					t.Error("selected store absent at listener construction")
					return
				}
				record, err := selected.SourceArtifactStore().GetSourceArtifact(context.Background(), verified.Hash)
				if err != nil {
					t.Error(err)
					return
				}
				artifact, err := record.Decode()
				if err != nil || !reflect.DeepEqual(artifact.MemberTable(), verified.Members) {
					t.Errorf("source not durably available before runtime construction: %v", err)
					return
				}
				durableBeforeConstruction = true
			}
			process, rt := start()
			defer process.stop()
			admitted, err := sourceartifact.AdmitDirectory(root)
			if err != nil || !strings.Contains(process.outputString(), admitted.HumanLabel()) {
				t.Fatalf("serve did not present admitted source: %v\n%s", err, process.outputString())
			}
			var blob []byte
			if err := rt.DB.QueryRow("SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1", verified.Hash).Scan(&blob); err != nil {
				t.Fatal(err)
			}
			stored, err := sourceartifact.DecodeLogical(blob)
			if err != nil || !durableBeforeConstruction || stored.BundleHash() != verified.Hash || rt.BundleHash != verified.Hash || !reflect.DeepEqual(stored.MemberTable(), verified.Members) {
				t.Fatalf("verify/serve disagree: verified=%+v stored=%v err=%v", verified, stored, err)
			}
		})
	}
}

func Test2376RootManifestRejectsBeforePublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, condition := range []string{"incompatible", "malformed range", "unknown path", "oversized name", "oversized version", "oversized platform_version"} {
			t.Run(backend+"/"+condition, func(t *testing.T) {
				root := canonicalrouting.CopyLifecycleForkSource(t, true)
				opts, start := lifecycleRestartHarness(t, backend, root)
				process, selected := start()
				before := lifecycleAdmissionDomainCounts(t, selected.DB)
				var sourcesBefore int
				if err := selected.DB.QueryRow("SELECT COUNT(*) FROM source_artifacts").Scan(&sourcesBefore); err != nil {
					t.Fatal(err)
				}
				if code := process.stop(); code != 0 {
					t.Fatalf("stop=%d", code)
				}
				want := "platform_version"
				if strings.HasPrefix(condition, "oversized ") {
					want = strings.TrimPrefix(condition, "oversized ")
					fields := map[string]string{"name": "Root", "version": "1.0.0", "platform_version": "*"}
					fields[want] = strings.Repeat("x", 257)
					write2376SourceFile(t, root, "manifest.yaml", fmt.Sprintf("name: %q\nversion: %q\nplatform_version: %q\n", fields["name"], fields["version"], fields["platform_version"]))
				} else if condition == "unknown path" {
					write2376SourceFile(t, root, "notes.txt", "relocate into docs\n")
					want = "notes.txt"
				} else {
					rangeText := ">=99.0.0"
					if condition == "malformed range" {
						rangeText = "not-a-range"
					}
					write2376SourceFile(t, root, "manifest.yaml", "name: Root\nversion: 1.0.0\nplatform_version: '"+rangeText+"'\n")
				}
				admissionEvidence := func(message string) bool {
					return strings.Contains(message, want) && (!strings.HasPrefix(condition, "oversized ") || strings.Contains(message, "256 UTF-8 bytes"))
				}
				var verifyOut, verifyErr bytes.Buffer
				if code := executeCLIFrom(context.Background(), root, []string{"verify", ".", "--config", opts.ConfigPath}, &verifyOut, &verifyErr, nil); code == 0 || !admissionEvidence(verifyErr.String()) {
					t.Fatalf("verify rejection=%d %s / %s", code, &verifyOut, &verifyErr)
				}
				var serveOut lockedBuffer
				published := false
				opts.Output = &serveOut
				opts.TestAPIListenerBound = func(net.Addr) { published = true }
				if code := runFrom(context.Background(), repoRootForTest(), *opts); code == 0 || published || !admissionEvidence(serveOut.String()) {
					t.Fatalf("serve rejection=%d published=%t\n%s", code, published, serveOut.String())
				}
				// The stopped connection is gone; read the same selected database afresh.
				var after map[string]int
				cfg, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{RepoRoot: repoRootForTest(), ExplicitPath: opts.ConfigPath})
				if err != nil {
					t.Fatal(err)
				}
				var sourcesAfter int
				if backend == "sqlite" {
					reader, err := store.NewSQLiteRuntimeStore(cfg.Config.Store.SQLite.Path)
					if err != nil {
						t.Fatal(err)
					}
					defer reader.Close()
					after = lifecycleAdmissionDomainCounts(t, storetest.DatabaseForTest(reader))
					if err := storetest.DatabaseForTest(reader).QueryRow("SELECT COUNT(*) FROM source_artifacts").Scan(&sourcesAfter); err != nil {
						t.Fatal(err)
					}
				} else {
					// The harness factory opens an independent selected owner for each epoch.
					owner, err := buildStoresForServe(context.Background(), storebackend.Selection{Backend: storebackend.BackendPostgres}, cfg.Config)
					if err != nil {
						t.Fatal(err)
					}
					defer owner.CloseUnactivated()
					db, _, _ := selectedRuntimeStoreForTest(t, projectRuntimePersistenceForServe(owner))
					after = lifecycleAdmissionDomainCounts(t, db)
					if err := db.QueryRow("SELECT COUNT(*) FROM source_artifacts").Scan(&sourcesAfter); err != nil {
						t.Fatal(err)
					}
				}
				if sourcesBefore != sourcesAfter || !reflect.DeepEqual(before, after) {
					t.Fatalf("rejection mutated domain: sources=%d/%d before=%v after=%v", sourcesBefore, sourcesAfter, before, after)
				}
			})
		}
	}
}

func Test2376ForkSourceAndPinsFollowSourceRunBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			previous := buildSelectedAPICapabilities
			buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
				caps, err := previous(owner, req)
				if err == nil {
					caps.RunFork = sourceForkErrorProbe{RunForkExecutor: caps.RunFork, t: t}
				}
				return caps, err
			}
			t.Cleanup(func() { buildSelectedAPICapabilities = previous })
			rootC := copy2376ForkSourceWithFeed(t)
			write2376SourceFile(t, rootC, "README.md", "Stored target C.\n")
			opts, start := lifecycleRestartHarness(t, backend, rootC)
			c, selectedC := start()
			hashC := selectedC.BundleHash
			if code := c.stop(); code != 0 {
				t.Fatalf("C stop=%d", code)
			}
			rootA := copy2376ForkSourceWithFeed(t)
			opts.SourceRoot = rootA
			a, selectedA := start()
			hashA := selectedA.BundleHash
			rows := filepath.Join(t.TempDir(), "rows.jsonl")
			if err := os.WriteFile(rows, []byte("{\"seed\":true}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			var importOut, importErr bytes.Buffer
			connection := writeCLIAPIConfigFile(t, map[string]string{"api_server": strings.TrimSuffix(selectedA.Endpoint, "/v1/rpc")})
			code := executeCLIFrom(context.Background(), t.TempDir(), []string{"--config", connection, "data", "import", "work.requested", rows, "--expected-head", "absent", "--source-invocation-id", uuid.NewString(), "--json"}, &importOut, &importErr, nil)
			out, errOut := importOut.String(), importErr.String()
			if code != 0 {
				t.Fatalf("public import=%d: %s / %s", code, out, errOut)
			}
			var imported durabledata.SourceOperationResult
			if err := json.Unmarshal([]byte(out), &imported); err != nil {
				t.Fatal(err)
			}
			if imported.Outcome != "accepted" || imported.Candidate.VersionID == "" || imported.BundleHash != hashA {
				t.Fatalf("missing imported version: %s", out)
			}
			dataCLI := func(args ...string) (string, string, int) {
				var out, errOut bytes.Buffer
				code := executeCLIFrom(context.Background(), t.TempDir(), append([]string{"--config", connection, "data"}, args...), &out, &errOut, nil)
				return out.String(), errOut.String(), code
			}
			shown, showErr, showCode := dataCLI("show", "work.requested@v1", "--json")
			if showCode != 0 || !strings.Contains(shown, string(imported.Candidate.VersionID)) {
				t.Fatalf("public exact show=%d %s / %s", showCode, shown, showErr)
			}
			pruned, pruneErr, pruneCode := dataCLI("prune", "work.requested@v1", "--expected-head", string(imported.Candidate.VersionID), "--prune-invocation-id", uuid.NewString(), "--json")
			var prune durabledata.PruneOperationResult
			if pruneCode != 0 || json.Unmarshal([]byte(pruned), &prune) != nil || prune.Outcome != "refused_current" || prune.VersionID != imported.Candidate.VersionID {
				t.Fatalf("public current-head fence=%d %s / %s", pruneCode, pruned, pruneErr)
			}
			type parent struct{ runID, frontierID string }
			parents := make(map[string]parent)
			for _, name := range []string{"default", "pin", "explicit", "missing", "contradictory"} {
				var seed servedEventPublishRPCResult
				if name == "pin" {
					seed.RunID = uuid.NewString()
					var out, errOut bytes.Buffer
					code := executeCLIFrom(context.Background(), t.TempDir(), []string{"run", "start", "--connect", strings.TrimSuffix(selectedA.Endpoint, "/v1/rpc"), "--pin", "work.requested@v1", "--run-id", seed.RunID, "--idempotency-key", "2376-pin-parent", "--no-follow"}, &out, &errOut, nil)
					if code != 0 {
						t.Fatalf("data parent start=%d %s / %s", code, &out, &errOut)
					}
				} else {
					out, errOut, code := runServedCLICommand(t, selectedA.Endpoint, []string{"event", "publish", "work.requested", "--payload-json", `{"seed":true}`, "--idempotency-key", "2376-" + name})
					if code != 0 {
						t.Fatalf("publish=%d %s / %s", code, out, errOut)
					}
					published := parseServedEventPublishOutput(t, out)
					seed = servedEventPublishRPCResult{RunID: published["run_id"], EventID: published["event_id"]}
				}
				var exact string
				if err := selectedA.DB.QueryRow("SELECT bundle_hash FROM runs WHERE run_id=$1", seed.RunID).Scan(&exact); err != nil || exact != hashA {
					t.Fatalf("new-work source=%s want=%s: %v", exact, hashA, err)
				}
				requireServedEventPublishEntityState(t, selectedA.DB, backend, seed.RunID, "", "review")
				waitServedRunDeliveryQuiescence(t, selectedA.DB, backend, seed.RunID)
				if name == "pin" {
					if err := selectedA.DB.QueryRow("SELECT event_id FROM events WHERE run_id=$1 AND event_name='work.requested' ORDER BY created_at,event_id LIMIT 1", seed.RunID).Scan(&seed.EventID); err != nil {
						t.Fatal(err)
					}
				}
				requireServedOKJSONRPC(t, selectedA.Endpoint, "run.pause", map[string]any{"run_id": seed.RunID, "idempotency_key": "pause-" + name})
				frontier := requireServedEventPublishRPCResult(t, selectedA.Endpoint, map[string]any{"event_name": "work.observed", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "frontier-" + name})
				parents[name] = parent{seed.RunID, frontier.EventID}
			}
			if code := a.stop(); code != 0 {
				t.Fatalf("A stop=%d", code)
			}
			rootB := copy2376ForkSourceWithFeed(t)
			write2376SourceFile(t, rootB, "README.md", "Serving source B, not the existing run's A.\n")
			opts.SourceRoot = rootB
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			b, servedB := start()
			defer b.stop()
			if servedB.BundleHash == hashA || servedB.BundleHash == hashC {
				t.Fatal("test did not select distinct A/B/C artifacts")
			}
			for _, name := range []string{"missing", "contradictory"} {
				t.Run(name, func(t *testing.T) {
					p := parents[name]
					args := []string{"run", "fork", p.runID, "--at-event", p.frontierID, "--allow-source-freeze", "--idempotency-key", "fork-" + name, "--json"}
					want := "--pin repeats"
					if name == "missing" {
						missing := copy2376ForkSourceWithFeed(t)
						write2376SourceFile(t, missing, "README.md", "Unstored source D.\n")
						args = append(args, "--source", missing)
						want = "serve"
					} else {
						args = append(args, "--pin", "work.requested@v1", "--pin", "work.requested@"+string(imported.Candidate.VersionID))
					}
					before := lifecycleAdmissionDomainCounts(t, servedB.DB)
					var beforeRun map[string]any
					requireServedJSONRPCResult(t, servedB.Endpoint, "run.get", map[string]any{"run_id": p.runID}, &beforeRun)
					out, errOut, code := runServedCLICommand(t, servedB.Endpoint, args)
					if code == 0 || !strings.Contains(errOut, want) {
						t.Fatalf("refusal=%d %s / %s", code, out, errOut)
					}
					var afterRun map[string]any
					requireServedJSONRPCResult(t, servedB.Endpoint, "run.get", map[string]any{"run_id": p.runID}, &afterRun)
					if !reflect.DeepEqual(beforeRun, afterRun) || !reflect.DeepEqual(before, lifecycleAdmissionDomainCounts(t, servedB.DB)) {
						t.Fatal("rejected source/pin mutated durable source or run")
					}
				})
			}
			for _, name := range []string{"default", "pin", "explicit"} {
				t.Run(name, func(t *testing.T) {
					p := parents[name]
					args := []string{"run", "fork", p.runID, "--at-event", p.frontierID, "--allow-source-freeze", "--idempotency-key", "fork-" + name, "--json"}
					want := hashA
					if name == "pin" {
						args = append(args, "--pin", "work.requested@v1")
					}
					if name == "explicit" {
						args = append(args, "--source", rootC)
						want = hashC
					}
					out, errOut, code := runServedCLICommand(t, servedB.Endpoint, args)
					if code != 0 {
						t.Fatalf("fork=%d %s / %s", code, out, errOut)
					}
					var fork apiv1.RunForkExecutionResult
					if err := json.Unmarshal([]byte(out), &fork); err != nil {
						t.Fatal(err)
					}
					var durableHash string
					if err := servedB.DB.QueryRow("SELECT bundle_hash FROM runs WHERE run_id=$1", fork.ForkRunID).Scan(&durableHash); err != nil {
						t.Fatal(err)
					}
					if fork.BundleHash != want || durableHash != want || fork.ExecutedEventCount != 1 {
						t.Fatalf("source selection was not exact: %+v / %s want %s", fork, durableHash, want)
					}
					var cardHash, cardVersion string
					if err := servedB.DB.QueryRow("SELECT bundle_hash,workflow_version FROM decision_cards WHERE run_id=$1 AND anchor_kind='stage_gate'", fork.ForkRunID).Scan(&cardHash, &cardVersion); err != nil || cardHash != want || cardVersion != want {
						t.Fatalf("child decision source=%s version=%s want=%s: %v", cardHash, cardVersion, want, err)
					}
					var wrongActivity, targetActivity int
					if err := servedB.DB.QueryRow("SELECT COUNT(*) FROM author_activity_occurrences WHERE run_id=$1 AND scope_kind='bundle' AND bundle_hash<>$2", fork.ForkRunID, want).Scan(&wrongActivity); err != nil || wrongActivity != 0 {
						t.Fatalf("child activity retained parent/serving source: %d %v", wrongActivity, err)
					}
					if err := servedB.DB.QueryRow("SELECT COUNT(*) FROM author_activity_occurrences WHERE run_id=$1 AND scope_kind='bundle' AND bundle_hash=$2", fork.ForkRunID, want).Scan(&targetActivity); err != nil || targetActivity == 0 {
						t.Fatalf("child has no exact activity source evidence: %d %v", targetActivity, err)
					}
					beforeRepeat := lifecycleAdmissionDomainCounts(t, servedB.DB)
					repeated, repeatErr, repeatCode := runServedCLICommand(t, servedB.Endpoint, args)
					var same apiv1.RunForkExecutionResult
					if repeatCode != 0 || json.Unmarshal([]byte(repeated), &same) != nil || same.ForkRunID != fork.ForkRunID || !reflect.DeepEqual(beforeRepeat, lifecycleAdmissionDomainCounts(t, servedB.DB)) {
						t.Fatalf("public exact repeat changed child effects: %d %s / %s", repeatCode, repeated, repeatErr)
					}
					if name == "pin" && (len(fork.DataPins) != 1 || fork.DataPins[0].VersionID != imported.Candidate.VersionID) {
						t.Fatalf("source pin lost: %+v", fork.DataPins)
					}
				})
			}
		})
	}
}

type sourceForkErrorProbe struct {
	apiv1.RunForkExecutor
	t *testing.T
}

func (p sourceForkErrorProbe) ExecuteRunFork(ctx context.Context, req apiv1.RunForkExecutionRequest) (apiv1.RunForkExecutionResult, error) {
	result, err := p.RunForkExecutor.ExecuteRunFork(ctx, req)
	if err != nil {
		p.t.Logf("selected fork refused: %v", err)
	}
	return result, err
}

func write2376SourceFile(t *testing.T, root, label, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, label), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

func copy2376ForkSourceWithFeed(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyLifecycleForkSource(t, true)
	raw, err := os.ReadFile(filepath.Join(root, "schema.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	write2376SourceFile(t, root, "schema.yaml", string(raw)+"  outputs:\n    events: [work.requested]\n")
	return root
}

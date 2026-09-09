package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestResetProcessDeathRecoversBeforeSourceAdmissionBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, phase := range []string{"admitted", "containers_settled"} {
			for _, clear := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/clear=%t", backend, phase, clear), func(t *testing.T) {
					unsetStoreSelectorEnv(t)
					source := writeServedEventPublishFollowUpFixture(t)
					hash := servedEventPublishFixtureBundleHash(t, source)
					var db *sql.DB
					var cfg, storeMode string
					if backend == servedparity.BackendDefaultSQLite {
						path := filepath.Join(t.TempDir(), "reset.db")
						cfg = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", path, channelOnboardingHostWorkspaceFields())
						var err error
						db, err = sql.Open("sqlite", path)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = db.Close() })
					} else {
						dsn, postgres, _ := testutil.StartEmptyPostgres(t)
						db, storeMode = postgres, "postgres"
						cfg = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
					}
					environment := []string{
						"SWARM_RESET_PROCESS_HELPER=1", "SWARM_RESET_CONFIG=" + cfg, "SWARM_RESET_SOURCE=" + source,
						"SWARM_RESET_STORE=" + storeMode, "SWARM_RESET_FAULT_PHASE=" + phase,
						"TMPDIR=" + t.TempDir(),
					}
					first := startServedCrashProcess(t, "TestResetCrashServeProcessHelper", append(environment, "SWARM_RESET_RECOVER=0"))
					endpoint := first.endpoint(t) + "/v1/rpc"
					initial := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
						"event_name": "item.received", "bundle_hash": hash,
						"payload": map[string]any{"item_id": "before-process-death"}, "idempotency_key": uuid.NewString(),
					})
					params := map[string]any{"include_source_artifacts": clear, "idempotency_key": uuid.NewString()}
					if response := requestServedJSONRPC(t, endpoint, "runtime.nuke", params); response.Error == nil {
						t.Fatal("reset receipt fault was not reached")
					}
					operation := readResetProcessOperation(t, db)
					if string(operation.Phase) != phase || len(operation.Request.SourceProjections) != 1 {
						t.Fatalf("missing exact admitted predecessor scope: %+v", operation)
					}
					if phase == "admitted" {
						if _, err := os.Stat(operation.Request.SourceProjections[0].Cleanup.Root); err != nil {
							t.Fatalf("predecessor projection already removed before crash: %v", err)
						}
					}
					if err := first.kill(); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(source, "schema.yaml"), []byte("invalid: ["), 0o600); err != nil {
						t.Fatal(err)
					}
					abandoned := append(append([]destructivereset.SourceProjection{}, operation.Request.SourceProjections...), operation.Allocations...)
					if !clear {
						if phase == "containers_settled" && len(operation.Allocations) != 1 {
							t.Fatalf("unreceipted successor allocation was not journaled: %+v", operation)
						}
						middle := startServedCrashProcessBeforeReadiness(t, "TestResetCrashServeProcessHelper", append(environment, "SWARM_RESET_RECOVER=1", "SWARM_RESET_BLOCK_RECOVERY=1"))
						deadline := time.After(serveRuntimeReadyTimeout)
						tick := time.NewTicker(10 * time.Millisecond)
						defer tick.Stop()
						for !strings.Contains(middle.output.String(), "RESET_RECOVERY_CANDIDATE_READY") {
							select {
							case <-middle.exited:
								t.Fatalf("recovery exited before reconstruction barrier: %v\n%s", middle.waitError(), middle.output.String())
							case <-deadline:
								t.Fatalf("recovery did not reach reconstruction barrier:\n%s", middle.output.String())
							case <-tick.C:
							}
						}
						interrupted := readResetProcessOperation(t, db)
						if interrupted.Phase != destructivereset.PhaseContainersSettled || len(interrupted.Allocations) != len(operation.Allocations)+1 || !reflect.DeepEqual(interrupted.Request, operation.Request) {
							t.Fatalf("recovery allocation missing or admission mutated: %+v", interrupted)
						}
						abandoned = append(abandoned, interrupted.Allocations...)
						if err := middle.kill(); err != nil {
							t.Fatal(err)
						}
					}
					recoveredEnv := append(append([]string{}, environment...), "SWARM_RESET_RECOVER=1")
					if !clear {
						recoveredEnv = append(recoveredEnv, "SWARM_RESET_FAULT_ALREADY_REMOVED=1")
					}
					second := startServedCrashProcess(t, "TestResetCrashServeProcessHelper", recoveredEnv)
					endpoint = second.endpoint(t) + "/v1/rpc"
					recovered := readResetProcessOperation(t, db)
					if recovered.Phase != destructivereset.PhaseCompleted || recovered.Request.OperationID != operation.Request.OperationID {
						t.Fatalf("reset did not recover its exact durable identity: %+v", recovered)
					}
					for _, projection := range abandoned {
						if _, err := os.Stat(projection.Cleanup.Root); !os.IsNotExist(err) {
							t.Fatalf("prior-process projection not settled: %v", err)
						}
					}
					var count int
					if err := db.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", initial.RunID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("predecessor persisted after reset recovery: %d, %v", count, err)
					}
					publish := map[string]any{"event_name": "item.received", "bundle_hash": hash, "payload": map[string]any{"item_id": "after-crash"}, "idempotency_key": uuid.NewString()}
					var later string
					if clear {
						response := requestServedJSONRPC(t, endpoint, "event.publish", publish)
						if response.Error == nil || response.Error.Data["code"] != apiv1.BundleUnavailableCode {
							t.Fatalf("cleared startup re-admitted execution: %+v", response.Error)
						}
					} else {
						later = requireServedEventPublishRPCResult(t, endpoint, publish).RunID
					}
					for i := 0; i < 2; i++ {
						if response := requestServedJSONRPC(t, endpoint, "runtime.nuke", params); response.Error != nil {
							t.Fatalf("post-crash replay: %+v", response.Error)
						}
						if later != "" {
							if err := db.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", later).Scan(&count); err != nil || count != 1 {
								t.Fatalf("replay affected later work: %d, %v", count, err)
							}
						}
					}
					if clear {
						for _, include := range []bool{false, true} {
							for _, keyed := range []bool{true, false} {
								fresh := map[string]any{"include_source_artifacts": include}
								if keyed {
									fresh["idempotency_key"] = uuid.NewString()
								}
								response := requestServedJSONRPC(t, endpoint, "runtime.nuke", fresh)
								if response.Error != nil {
									t.Fatalf("fresh empty-topology reset include=%t keyed=%t: %+v", include, keyed, response.Error)
								}
								if keyed {
									replay := requestServedJSONRPC(t, endpoint, "runtime.nuke", fresh)
									if replay.Error != nil || !reflect.DeepEqual(replay.Result, response.Result) {
										t.Fatalf("empty reset replay changed outcome: %+v -> %+v", response, replay)
									}
									fresh["include_source_artifacts"] = !include
									if conflict := requestServedJSONRPC(t, endpoint, "runtime.nuke", fresh); conflict.Error == nil {
										t.Fatal("conflicting empty reset replay accepted")
									}
								}
							}
						}
						if err := db.QueryRow("SELECT COUNT(*) FROM runtime_reset_operations WHERE active_slot IS NOT NULL").Scan(&count); err != nil || count != 0 {
							t.Fatalf("empty reset poisoned active slot: %d, %v", count, err)
						}
						if response := requestServedJSONRPC(t, endpoint, "event.publish", publish); response.Error == nil || response.Error.Data["code"] != apiv1.BundleUnavailableCode {
							t.Fatalf("empty reset resurrected source: %+v", response)
						}
					}
					if err := second.stop(); err != nil {
						t.Fatalf("recovered process shutdown: %v\n%s", err, second.output.String())
					}
				})
			}
		}
	}
}

func readResetProcessOperation(t *testing.T, db *sql.DB) destructivereset.Operation {
	t.Helper()
	var encoded []byte
	if err := db.QueryRow("SELECT record FROM runtime_reset_operations").Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var operation destructivereset.Operation
	if err := json.Unmarshal(encoded, &operation); err != nil {
		t.Fatal(err)
	}
	if err := operation.Validate(); err != nil {
		t.Fatal(err)
	}
	return operation
}

func TestResetCrashServeProcessHelper(t *testing.T) {
	if os.Getenv("SWARM_RESET_PROCESS_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	fault := storetest.SetResetFinalReceiptFault
	if os.Getenv("SWARM_RESET_FAULT_PHASE") == "admitted" {
		fault = storetest.SetResetPlanReceiptFault
	}
	var selected any
	captureSelectedRuntimePersistence(t, func(persistence serveRuntimePersistence) {
		_, pg, sqlite := selectedRuntimeStoreForTest(t, persistence)
		if sqlite != nil {
			selected = sqlite
		} else {
			selected = pg
		}
		if os.Getenv("SWARM_RESET_RECOVER") == "1" && os.Getenv("SWARM_RESET_FAULT_ALREADY_REMOVED") != "1" {
			if err := fault(context.Background(), selected, false); err != nil {
				t.Fatal(err)
			}
		}
	})
	opts := cliapp.DefaultServeOptions()
	opts.ConfigPath = os.Getenv("SWARM_RESET_CONFIG")
	opts.SourceRoot = os.Getenv("SWARM_RESET_SOURCE")
	opts.PlatformSpecPath = defaultPlatformSpecPath
	opts.StoreMode = os.Getenv("SWARM_RESET_STORE")
	opts.StoreModeSet = opts.StoreMode != ""
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.Verbose, opts.SelfCheck = true, true
	opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
	if os.Getenv("SWARM_RESET_RECOVER") != "1" {
		opts.TestRuntimeReadyHook = func(*runtime.Runtime) {
			if err := fault(context.Background(), selected, true); err != nil {
				t.Fatal(err)
			}
		}
	} else if os.Getenv("SWARM_RESET_BLOCK_RECOVERY") == "1" {
		opts.TestRuntimeReadyHook = func(*runtime.Runtime) {
			fmt.Fprintln(os.Stdout, "RESET_RECOVERY_CANDIDATE_READY")
			select {}
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if code := runFrom(ctx, repoRootForTest(), opts); code != 0 {
		t.Fatalf("reset process exit=%d", code)
	}
}

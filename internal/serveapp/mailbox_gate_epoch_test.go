package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/servedparity"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/selected/selectedtest"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/google/uuid"
)

func waitMailboxCardDecision(t *testing.T, endpoint, cardID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var detail struct {
			Card struct {
				CardID          string `json:"card_id"`
				Status, Verdict string
			} `json:"decision_card"`
		}
		requireServedJSONRPCResult(t, endpoint, "mailbox.get", map[string]any{"card_id": cardID}, &detail)
		if detail.Card.CardID != cardID {
			t.Fatalf("mailbox returned another card: %+v", detail)
		}
		if detail.Card.Status == "decided" && detail.Card.Verdict == "approve" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("authenticated channel action did not decide actual card: %+v", detail)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type mailboxGateReturn struct {
	Calls  int
	Result any
	Err    error
	Domain gateCompletionDomain
	CardID string
}

type mailboxGateEpoch struct {
	Endpoint, BundleHash, RuntimeInstanceID string
	Base                                    decisioncard.Card
	Card                                    func(string) (decisioncard.Card, error)
	Observe                                 func(string, string, string, gateCompletionDomain) selectedtest.MailboxGateEvidence
	Activity                                func(authoractivity.ListOptions) (authoractivity.ListResult, error)
	Prepared                                func(string) (bus.PreparedPublishEvent, bool, error)
	WaitDeliveries                          func(string)
	WaitStage                               func(string, string)
	Unloaded                                func(string, string)
	Capture                                 func() mailboxGateReturn
	Join                                    func()
}

func newMailboxGateEpoch(t *testing.T, backend servedparity.Backend, root string) (mailboxGateEpoch, func() mailboxGateEpoch) {
	t.Helper()
	name, location := "sqlite", filepath.Join(t.TempDir(), "completion.sqlite")
	if backend == servedparity.BackendExplicitPostgres {
		name, location = "postgres", testutil.StartEmptyPostgresDSN(t)
	} else {
		unsetStoreSelectorEnv(t)
	}
	opts := cliapp.ServeOptions{SourceRoot: root, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	opts.ConfigPath = writeMockAgentRuntimeConfig(t, name, location)
	var fixture selectedtest.MailboxGateFixture
	previousProjection := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		var err error
		fixture, err = selectedtest.BindMailboxGateFixtureForTest(owner)
		if err != nil {
			t.Fatal(err)
		}
		return previousProjection(owner)
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previousProjection })
	if name == "postgres" {
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, name, "")
		opts.StoreMode, opts.StoreModeSet = name, true
		previous := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: selection, PostgresDSN: location, SessionLockTTL: runtimeSessionLockTTL(cfg)})
		}
		t.Cleanup(func() { buildStoresForServe = previous })
	}
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	writeChannelOnboardingLiveToolCredentials(t, credentialPath)
	retained := t.TempDir()
	t.Cleanup(func() {
		if err := removePrivateTestRoot(retained); err != nil {
			t.Error(err)
		}
	})
	var process *serveRuntimeTestProcess
	var join func()
	var capture struct {
		sync.Mutex
		value mailboxGateReturn
	}
	start := func() mailboxGateEpoch {
		if join != nil {
			join()
		}
		captured := captureSelectedResetSupervisor(t)
		process = startRuntimeTestProcessWithRunner(t, repoRootForTest(), opts, func(ctx context.Context, repo string, opts cliapp.ServeOptions) int {
			code, err := runOwnedLifecycle(ctx, repo, retained, opts, apiv1.AuthTokenResolution{Tokens: []string{apiv1.DefaultLoopbackAPIToken}, Explicit: true, Source: "internal-lifecycle-parent"}, executionposture.MockOnly, "")
			if err != nil {
				fmt.Fprintf(opts.ErrorOutput, "internal mailbox lifecycle setup: %v\n", err)
			}
			return code
		})
		current := process
		current.waitForReadyLine()
		rt := servedTestProcessRuntime(t, current)
		observed := fixture.Observation
		supervisor := <-captured
		supervisor.mu.Lock()
		original := supervisor.execution["mailbox.decide"]
		if original == nil {
			supervisor.mu.Unlock()
			t.Fatal("mailbox epoch has no actual decision handler")
		}
		supervisor.execution["mailbox.decide"] = func(ctx context.Context, req apiv1.Request) (any, error) {
			result, err := original(ctx, req)
			key, _ := req.Params["idempotency_key"].(string)
			cardID, _ := req.Params["card_id"].(string)
			capture.Lock()
			capture.value.Calls++
			capture.value.Result, capture.value.Err = result, err
			capture.value.Domain = gateCompletionDomain{Method: req.Method, Actor: req.OperatorPrincipalID, Key: key, Hash: req.RequestHash}
			capture.value.CardID = cardID
			capture.Unlock()
			return result, err
		}
		supervisor.mu.Unlock()
		active := true
		guard := func() error {
			if !active {
				return fmt.Errorf("mailbox observation belongs to a stopped epoch")
			}
			return nil
		}
		scope := servedRuntimeProofAuthorActivityContext(t, rt, rt.Options.SourceArtifactFact.BundleHash())
		epoch := mailboxGateEpoch{Endpoint: "http://" + serveRuntimeAPIListenerFromOutput(t, current.outputString()) + "/v1/rpc", BundleHash: rt.Options.SourceArtifactFact.BundleHash(), RuntimeInstanceID: rt.Options.RuntimeInstanceID}
		epoch.Join = func() {
			if !active {
				return
			}
			active = false
			if code := current.stop(); code != 0 {
				t.Errorf("joined mailbox runtime exit=%d\n%s", code, current.outputString())
			}
		}
		join = epoch.Join
		t.Cleanup(epoch.Join)
		epoch.Card = func(id string) (decisioncard.Card, error) {
			if err := guard(); err != nil {
				return decisioncard.Card{}, err
			}
			return observed.Card(scope, id)
		}
		epoch.Activity = func(opts authoractivity.ListOptions) (authoractivity.ListResult, error) {
			if err := guard(); err != nil {
				return authoractivity.ListResult{}, err
			}
			return observed.Activity(scope, opts)
		}
		epoch.Prepared = func(id string) (bus.PreparedPublishEvent, bool, error) {
			if err := guard(); err != nil {
				return bus.PreparedPublishEvent{}, false, err
			}
			return observed.Prepared(scope, id)
		}
		epoch.Observe = func(phase, run, card string, domain gateCompletionDomain) selectedtest.MailboxGateEvidence {
			if err := guard(); err != nil {
				t.Fatal(err)
			}
			out, err := observed.Observe(scope, run, card, selectedtest.MailboxGateDomain(domain))
			if err != nil {
				t.Fatal(err)
			}
			out.Phase = phase
			raw, err := json.Marshal(out)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("GATE_COMPLETION_RECORD %s", raw)
			return out
		}
		epoch.WaitDeliveries = func(run string) {
			if err := guard(); err != nil {
				t.Fatal(err)
			}
			waitServedRunDeliveryQuiescence(t, observed.Deliveries, run)
		}
		epoch.WaitStage = func(run, want string) {
			if err := guard(); err != nil {
				t.Fatal(err)
			}
			var last struct {
				Instances []operatorread.RunInstanceOperationalStatus `json:"instances"`
			}
			for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
				requireServedJSONRPCResult(t, epoch.Endpoint, "run.diagnose", map[string]any{"run_id": run}, &last)
				matches := 0
				for _, instance := range last.Instances {
					if instance.FlowInstance == run && instance.Stage == want {
						matches++
					}
				}
				if matches == 1 {
					return
				}
				if matches > 1 {
					t.Fatalf("duplicate exact root instance: %+v", last)
				}
				time.Sleep(50 * time.Millisecond)
			}
			t.Fatalf("exact run %s root did not reach %s: %+v", run, want, last)
		}
		epoch.Unloaded = func(run, hash string) {
			if err := guard(); err != nil {
				t.Fatal(err)
			}
			lookup, availability, err := supervisor.runtimeContexts.LookupRunStatus(t.Context(), run)
			if err != nil || lookup.Loaded() || availability.Status != "completed" || availability.BundleHash != hash {
				t.Fatalf("completed replay fixture is not exact/unloaded: lookup=%+v availability=%+v error=%v", lookup, availability, err)
			}
		}
		epoch.Capture = func() mailboxGateReturn { capture.Lock(); defer capture.Unlock(); return capture.value }
		return epoch
	}
	first := start()
	rt := servedTestProcessRuntime(t, process)
	source := rt.Pipeline.SemanticSource()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("publication setup requires actual admitted source")
	}
	runID := uuid.NewString()
	ctx := effects.WithExecutionMode(correlation.WithRunID(servedRuntimeProofAuthorActivityContext(t, rt, first.BundleHash), runID), rt.ExecutionPosture.RootMode())
	if err := fixture.RequirePublicationRun(ctx, runID, bundle.SourceArtifact); err != nil {
		t.Fatal(err)
	}
	plan, err := rt.Manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, OccurredAt: time.Now().UTC(), Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, "")})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := rt.Bus.CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("commit publication setup: acknowledged=%t created=%t err=%v", committed.Acknowledged, committed.Created, err)
	}
	if err := rt.Manager.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
		t.Fatal(err)
	}
	seed := requireServedEventPublishRPCResult(t, first.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": first.BundleHash, "run_id": runID, "payload": map[string]any{"seed": true}, "idempotency_key": "matrix-seed-" + uuid.NewString()})
	if seed.RunID != runID || seed.NewRunCreated {
		t.Fatalf("publication escaped exact native fixture run: %+v", seed)
	}
	first.WaitStage(runID, "review")
	first.WaitDeliveries(runID)
	params := map[string]any{"status": "pending", "limit": 100}
	var id string
	for {
		var page servedEntityFilterList
		requireServedJSONRPCResult(t, first.Endpoint, "mailbox.list", params, &page)
		for _, item := range page.Items {
			if item.Card.RunID == runID {
				if id != "" {
					t.Fatal("native publication produced multiple cards")
				}
				id = item.Card.CardID
			}
		}
		if page.NextCursor == "" {
			break
		}
		params["cursor"] = page.NextCursor
	}
	if id == "" {
		t.Fatal("native publication produced no waiting card")
	}
	first.Base, err = first.Card(id)
	if err != nil {
		t.Fatal(err)
	}
	return first, start
}

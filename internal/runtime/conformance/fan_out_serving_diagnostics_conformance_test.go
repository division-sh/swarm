package conformance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

const servingD3Code = "fan_out_claim_opportunity_missed"

type servingD3LogReceipt struct {
	runID   string
	failure *failures.Envelope
	err     error
}

type servingD3Logger struct {
	conformanceRuntimeLoggerHook
	receipts chan servingD3LogReceipt
}

func (l *servingD3Logger) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlated map[string]string, detail any, failure *failures.Envelope, durationUS int) error {
	// Observe the real logger outcome without supplying missing run/source
	// identity or constructing a replacement diagnostic event.
	err := l.conformanceRuntimeLoggerHook.Log(ctx, level, message, component, action, eventID, eventType, agentID, entityID, sessionID, correlated, detail, failure, durationUS)
	if failure != nil && failure.Detail.Code == servingD3Code {
		runID := correlation.RunIDFromContext(ctx)
		if lineage, ok := correlation.RuntimeLineageFromContext(ctx); runID == "" && ok {
			runID = lineage.RunID
		}
		l.receipts <- servingD3LogReceipt{runID: runID, failure: failures.CloneEnvelope(failure), err: err}
	}
	return err
}

type servingD3Reader interface {
	ListOperatorRuntimeLogs(context.Context, operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error)
	ListOperatorRuntimeIncidents(context.Context, operatorread.OperatorRuntimeIncidentListOptions) (operatorread.OperatorRuntimeIncidentListResult, error)
}

func installServingD3Logger(t *testing.T, f *servingMatrixFixture, index int) *servingD3Logger {
	t.Helper()
	persistence, ok := f.selected.(runtimepkg.RuntimeLogPersistence)
	if !ok {
		t.Fatalf("selected store %T lacks the actual runtime logger persistence owner", f.selected)
	}
	rt := f.runtimes[index]
	logger := &servingD3Logger{
		conformanceRuntimeLoggerHook: conformanceRuntimeLoggerHook{logger: runtimepkg.NewRuntimeLogger(persistence, rt.posture,
			runtimepkg.NewRuntimePayloadAdmitter(nil, f.sources[index], rt.sourceArtifactFact))},
		receipts: make(chan servingD3LogReceipt, 32),
	}
	rt.bus.SetLoggerHook(logger)
	return logger
}

func waitServingD3Log(t *testing.T, logger *servingD3Logger) servingD3LogReceipt {
	t.Helper()
	select {
	case receipt := <-logger.receipts:
		return receipt
	case <-time.After(5 * time.Second):
		t.Fatal("independent D3 detector did not reach the real runtime logger while the actual claim remained held")
		return servingD3LogReceipt{}
	}
}

func assertServingD3Quiet(t *testing.T, logger *servingD3Logger) {
	t.Helper()
	// Longer than the one-second opportunity bound plus two production samples.
	select {
	case receipt := <-logger.receipts:
		t.Fatalf("unexpected D3 episode: run=%q failure=%+v persistence=%v", receipt.runID, receipt.failure, receipt.err)
	case <-time.After(1750 * time.Millisecond):
	}
}

func assertNoServingD3Report(t *testing.T, probe *servingMatrixProbe) {
	t.Helper()
	probe.mu.Lock()
	defer probe.mu.Unlock()
	for _, err := range probe.errors {
		failure := failures.Normalize(err, "runtime.fan_out", "test_observation")
		if failure.Detail.Code == servingD3Code {
			t.Fatalf("false D3 reached the runtime executor, even if log persistence was delayed: %+v", failure)
		}
	}
}

func TestIssue2394D3IncidentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pending := newServingMatrixProbe(servingMatrixClaim, 1)
			sibling := newServingMatrixProbe(servingMatrixUnheld, 0)
			workers := 1
			if backend == "postgres" {
				workers = 2
			}
			f := newServingMatrixFixture(t, backend, &workers, pending, sibling)
			logger := installServingD3Logger(t, f, 0)
			siblingLogger := installServingD3Logger(t, f, 1)
			ctx, runID := f.startRun(t, 0)
			_, siblingRun := f.startRun(t, 1)
			trigger := f.submit(t, 0, runID, "d3-held-claim")
			turn := waitServingMatrixHeld(t, pending)
			defer turn.release()
			waitServingMatrixTriggerReceipt(t, f.db, trigger)
			assertServingLifetimeUnissued(t, readServingLifetimeState(t, f.db, runID))
			select {
			case attempt := <-pending.attempts:
				t.Fatalf("D3 setup entered actual claim prematurely: %+v", attempt)
			default:
			}
			f.submit(t, 1, siblingRun, "d3-unrelated-attempt")
			siblingIntents := 1
			if backend == "postgres" {
				receipt := waitServingMatrixReceipt(t, sibling, time.Now().Add(5*time.Second))
				if receipt.err != nil {
					t.Fatal(receipt.err)
				}
				select {
				case attempt := <-sibling.attempts:
					if attempt.key.RunID != siblingRun {
						t.Fatalf("unrelated real attempt has wrong run: %+v", attempt)
					}
				default:
					t.Fatal("unrelated real turn completed without an actual claim attempt")
				}
			}
			var receipt servingD3LogReceipt
			if backend == "postgres" {
				// Keep unrelated actual claims entering throughout A's opportunity
				// episode. A global last-attempt clock would suppress A forever.
				ticker := time.NewTicker(300 * time.Millisecond)
				defer ticker.Stop()
				deadline := time.NewTimer(5 * time.Second)
				defer deadline.Stop()
			waitEpisode:
				for {
					select {
					case receipt = <-logger.receipts:
						break waitEpisode
					case <-ticker.C:
						siblingIntents++
						f.submit(t, 1, siblingRun, fmt.Sprintf("d3-unrelated-%d", siblingIntents))
						completed := waitServingMatrixReceipt(t, sibling, time.Now().Add(5*time.Second))
						if completed.err != nil {
							t.Fatal(completed.err)
						}
					case <-deadline.C:
						t.Fatal("unrelated real B claims reset or suppressed A's independently observed D3 episode")
					}
				}
			} else {
				receipt = waitServingD3Log(t, logger)
			}
			if receipt.runID != runID {
				t.Errorf("D3 report lost exact candidate run context: context run=%q candidate run=%q failure=%+v persistence=%v", receipt.runID, runID, receipt.failure, receipt.err)
			}
			if receipt.err != nil {
				t.Errorf("actual D3 runtime log persistence failed: %v", receipt.err)
			}
			grant, err := f.topology.grants[f.runtimes[0].sourceArtifactFact.BundleHash()].Evidence()
			if err != nil {
				t.Fatal(err)
			}
			assertServingD3Failure(t, receipt.failure, turn.key, grant.GrantID)
			assertServingD3Quiet(t, logger)
			assertServingD3Projection(t, f, ctx, turn.key, grant.GrantID)
			assertServingD3SupportedReadback(t, f, ctx, turn.key, grant.GrantID)
			assertServingLifetimeUnissued(t, readServingLifetimeState(t, f.db, runID))
			turn.release()
			f.assertSettled(t, 0, runID, 1)
			f.assertSettled(t, 1, siblingRun, siblingIntents)
			select {
			case extra := <-siblingLogger.receipts:
				t.Errorf("unrelated source gained a D3 episode: %+v", extra)
			default:
			}
			if !t.Failed() {
				t.Logf("M12/%s: actual pre-claim stall -> independent detector -> RuntimeLogger -> scoped runtime.logs/runtime.incidents; exactly one episode; no issuance by the diagnostic", backend)
			}
		})
	}
}

func assertServingD3Failure(t *testing.T, failure *failures.Envelope, key fanoutobligation.IntentKey, grantID string) {
	t.Helper()
	if failure == nil || failures.ValidateEnvelope(*failure) != nil || failure.Class != failures.ClassInternalFailure || failure.Detail.Code != servingD3Code || failure.Component != "runtime.fan_out" || failure.Operation != "observe_opportunity" {
		t.Errorf("D3 lost its canonical typed failure: %+v", failure)
		return
	}
	for name, want := range map[string]string{
		"grant_id": grantID, "run_id": key.RunID, "triggering_delivery_id": key.TriggeringDeliveryID,
		"flow_path": key.ElementRef.FlowPath, "declaration_family": key.ElementRef.Family, "semantic_path": key.ElementRef.SemanticPath,
		"claim_attempt_bound_ms": "1000",
	} {
		if got := fmt.Sprint(failure.Detail.Attributes[name]); got != want {
			t.Errorf("D3 exact %s=%q want=%q", name, got, want)
		}
	}
}

func assertServingD3Projection(t *testing.T, f *servingMatrixFixture, ctx context.Context, key fanoutobligation.IntentKey, grantID string) {
	t.Helper()
	reader := f.selected.(servingD3Reader)
	bundle := f.runtimes[0].sourceArtifactFact.BundleHash()
	logs, err := reader.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{
		RunID: key.RunID, BundleHash: bundle, Component: "workflow-runtime", Level: "warn", Limit: 10,
	})
	if err != nil {
		t.Errorf("D3 runtime.logs read: %v", err)
	} else if len(logs.Logs) != 1 || logs.NextCursor != "" {
		t.Errorf("D3 runtime.logs lost/duplicated scoped episode: %+v", logs)
	} else {
		log := logs.Logs[0]
		if log.RunID != key.RunID || log.ErrorCode != servingD3Code || log.Level != "warn" || log.Component != "workflow-runtime" || log.Action != "serve_fan_out_obligation" {
			t.Errorf("D3 log fields=%+v", log)
		}
		assertServingD3Failure(t, log.Failure, key, grantID)
	}
	incidents, err := reader.ListOperatorRuntimeIncidents(ctx, operatorread.OperatorRuntimeIncidentListOptions{BundleHash: bundle, Component: "workflow-runtime", Level: "warn", SinceHours: 1, Limit: 10})
	if err != nil {
		t.Errorf("D3 runtime.incidents read: %v", err)
		return
	}
	count := 0
	for _, incident := range incidents.Incidents {
		if incident.ErrorCode != servingD3Code {
			continue
		}
		count++
		if incident.Count != 1 || len(incident.SampleLogIDs) != 1 || incident.FirstSeen.IsZero() || !incident.FirstSeen.Equal(incident.LastSeen) {
			t.Errorf("D3 incident is not one canonical log episode: %+v", incident)
		}
		if len(logs.Logs) == 1 && len(incident.SampleLogIDs) == 1 && incident.SampleLogIDs[0] != logs.Logs[0].LogID {
			t.Errorf("D3 incident did not reference actual runtime log: %+v", incident)
		}
	}
	if count != 1 {
		t.Errorf("D3 runtime.incidents lost/duplicated scoped failure: %+v", incidents)
	}
	foreign, err := reader.ListOperatorRuntimeIncidents(ctx, operatorread.OperatorRuntimeIncidentListOptions{BundleHash: f.runtimes[1].sourceArtifactFact.BundleHash(), SinceHours: 1, Limit: 10})
	if err != nil {
		t.Error(err)
	}
	for _, incident := range foreign.Incidents {
		if incident.ErrorCode == servingD3Code {
			t.Errorf("D3 incident leaked into unrelated source: %+v", incident)
		}
	}
}

func TestIssue2394D3EvidenceControlsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, control := range []string{"idle", "admitted_capacity_saturated", "slow_claim_sql", "paused_preclaim", "retired_preclaim"} {
				t.Run(control, func(t *testing.T) {
					phase := servingMatrixClaim
					if control == "admitted_capacity_saturated" {
						phase = servingMatrixEvaluation
					}
					probe := newServingMatrixProbe(phase, 1)
					workers := 1
					f := newServingMatrixFixture(t, backend, &workers, probe)
					logger := installServingD3Logger(t, f, 0)
					ctx, runID := f.startRun(t, 0)
					var waitingRun string
					if control == "admitted_capacity_saturated" {
						_, waitingRun = f.startRun(t, 0)
					}
					if control == "idle" {
						assertServingD3Quiet(t, logger)
						assertNoServingD3Report(t, probe)
						return
					}
					trigger := f.submit(t, 0, runID, "d3-evidence-control")
					turn := waitServingMatrixHeld(t, probe)
					defer turn.release()
					waitServingMatrixTriggerReceipt(t, f.db, trigger)
					switch control {
					case "slow_claim_sql":
						// Lock the existing mutation-order row without changing it.
						// The actual claim must enter the owner and wait on SQL, not
						// remain in this test's pre-entry gate.
						tx, err := f.db.BeginTx(ctx, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Rollback()
						if _, err := tx.ExecContext(ctx, `UPDATE author_activity_order SET last_sequence=last_sequence WHERE singleton_id=1`); err != nil {
							t.Fatal(err)
						}
						turn.release()
						select {
						case attempt := <-probe.attempts:
							if attempt.key != turn.key {
								t.Fatalf("wrong slow-SQL claim: %+v", attempt)
							}
						case <-time.After(5 * time.Second):
							t.Fatal("real claim did not enter the selected owner")
						}
						assertServingD3Quiet(t, logger)
						assertNoServingD3Report(t, probe)
						select {
						case completed := <-probe.completed:
							t.Fatalf("claim bypassed held SQL mutation order: %+v", completed)
						default:
						}
						if err := tx.Rollback(); err != nil {
							t.Fatal(err)
						}
						f.assertSettled(t, 0, runID, 1)
					case "admitted_capacity_saturated":
						if turn.claim.Generation == 0 || turn.loadedAt.IsZero() {
							t.Fatal("saturation control must enter the actual store claim and evaluation")
						}
						f.submit(t, 0, waitingRun, "d3-capacity-waiter")
						waitServingMatrixIntentCount(t, f, 2)
						assertServingD3Quiet(t, logger)
						turn.release()
						f.assertSettled(t, 0, runID, 1)
						f.assertSettled(t, 0, waitingRun, 1)
					case "paused_preclaim":
						controlOwner := f.selected.(interface {
							PauseRunControl(context.Context, runcontrol.TransitionRequest) (runcontrol.State, error)
							ContinueRunControl(context.Context, runcontrol.TransitionRequest) (runcontrol.State, error)
						})
						transition := runcontrol.TransitionRequest{RunID: runID, Now: time.Now().UTC(), Reason: "d3-control", ControlledBy: "conformance"}
						if _, err := controlOwner.PauseRunControl(ctx, transition); err != nil {
							t.Fatal(err)
						}
						assertServingD3Quiet(t, logger)
						transition.Now = time.Now().UTC()
						if _, err := controlOwner.ContinueRunControl(ctx, transition); err != nil {
							t.Fatal(err)
						}
						turn.release()
						f.assertSettled(t, 0, runID, 1)
					case "retired_preclaim":
						grant := f.topology.grants[f.runtimes[0].sourceArtifactFact.BundleHash()]
						if err := grant.Retire(ctx); err != nil {
							t.Fatal(err)
						}
						// Close joins this deliberately held caller; release must not wait on Close.
						closed := make(chan struct{})
						go func() {
							f.runtimes[0].fanOutServing.Close()
							close(closed)
						}()
						assertServingD3Quiet(t, logger)
						select {
						case <-closed:
							t.Fatal("registration Close returned before the held caller exited")
						default:
						}
						turn.release()
						deadline := time.Now().Add(5 * time.Second)
						waitServingMatrixReceipt(t, probe, deadline)
						select {
						case <-closed:
						case <-time.After(time.Until(deadline)):
							t.Fatal("registration Close did not finish after the held caller exited")
						}
					}
					assertNoServingD3Report(t, probe)
					reader := f.selected.(servingD3Reader)
					logs, err := reader.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{Component: "workflow-runtime", Level: "warn", Limit: 100})
					if err != nil || logs.NextCursor != "" {
						t.Fatalf("M13/%s diagnostic read incomplete: logs=%+v err=%v", control, logs, err)
					}
					for _, log := range logs.Logs {
						if log.ErrorCode == servingD3Code {
							t.Fatalf("M13/%s acquired an unwarranted persisted D3 episode: %+v", control, log)
						}
					}
					t.Logf("M13/%s/%s: no diagnostic across the production opportunity bound", backend, control)
				})
			}
		})
	}
}

func TestIssue2394D3FailedReadEpisodeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			probe := newServingMatrixProbe(servingMatrixClaim, 1)
			workers := 1
			f := newServingMatrixFixture(t, backend, &workers, probe)
			logger := installServingD3Logger(t, f, 0)
			ctx, runID := f.startRun(t, 0)
			trigger := f.submit(t, 0, runID, "d3-read-failure")
			turn := waitServingMatrixHeld(t, probe)
			defer turn.release()
			waitServingMatrixTriggerReceipt(t, f.db, trigger)
			first := waitServingD3Log(t, logger)
			if first.err != nil || first.runID != runID {
				t.Fatalf("first real episode: %+v", first)
			}
			// This isolated database fault makes the production observer's real
			// query fail. It neither injects a diagnostic nor changes its clock.
			if _, err := f.db.ExecContext(ctx, `ALTER TABLE fan_out_intents RENAME TO fan_out_intents_d3_unavailable`); err != nil {
				t.Fatal(err)
			}
			restored := false
			defer func() {
				if !restored {
					if _, err := f.db.ExecContext(context.Background(), `ALTER TABLE fan_out_intents_d3_unavailable RENAME TO fan_out_intents`); err != nil {
						t.Error(err)
					}
				}
			}()
			if _, err := f.db.ExecContext(ctx, `SELECT run_id FROM fan_out_intents LIMIT 1`); err == nil {
				t.Fatal("observation table fault did not reach the actual database")
			}
			assertServingD3Quiet(t, logger)
			if _, err := f.db.ExecContext(ctx, `ALTER TABLE fan_out_intents_d3_unavailable RENAME TO fan_out_intents`); err != nil {
				t.Fatal(err)
			}
			restored = true
			select {
			case premature := <-logger.receipts:
				t.Fatalf("unknown read interval counted as an uninterrupted opportunity: %+v", premature)
			case <-time.After(750 * time.Millisecond):
			}
			second := waitServingD3Log(t, logger)
			if second.err != nil || second.runID != runID {
				t.Fatalf("renewed real episode: %+v", second)
			}
			reader := f.selected.(servingD3Reader)
			bundle := f.runtimes[0].sourceArtifactFact.BundleHash()
			logs, err := reader.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{RunID: runID, BundleHash: bundle, Component: "workflow-runtime", Level: "warn", Limit: 100})
			if err != nil || logs.NextCursor != "" {
				t.Fatalf("episode logs=%+v err=%v", logs, err)
			}
			ids := map[string]bool{}
			for _, log := range logs.Logs {
				if log.ErrorCode == servingD3Code {
					if log.RunID != runID || ids[log.LogID] {
						t.Fatalf("wrong episode identity: %+v", log)
					}
					ids[log.LogID] = true
				}
			}
			if len(ids) != 2 {
				t.Fatalf("failed read did not separate exactly two actual episodes: %+v", logs)
			}
			incidents, err := reader.ListOperatorRuntimeIncidents(ctx, operatorread.OperatorRuntimeIncidentListOptions{BundleHash: bundle, Component: "workflow-runtime", Level: "warn", SinceHours: 1, Limit: 100})
			if err != nil || incidents.NextCursor != "" {
				t.Fatalf("episode incidents=%+v err=%v", incidents, err)
			}
			found := false
			for _, incident := range incidents.Incidents {
				if incident.ErrorCode != servingD3Code {
					continue
				}
				if found || incident.Count != 2 || len(incident.SampleLogIDs) != 2 {
					t.Fatalf("episode aggregation=%+v", incident)
				}
				found = true
				for _, id := range incident.SampleLogIDs {
					if !ids[id] {
						t.Fatalf("incident sampled a non-episode log: %s", id)
					}
				}
			}
			if !found {
				t.Fatal("two real log episodes did not reach the incident projection")
			}
			turn.release()
			f.assertSettled(t, 0, runID, 1)
			t.Logf("M13/%s: actual observation read failures ended the prior episode; recovery waited for a new bound; two canonical logs grouped into one incident", backend)
		})
	}
}

func TestIssue2394D3CorruptHeaderNoPositiveBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, corruption := range []string{"orphan_lease", "invalid_retry_failure"} {
			t.Run(backend+"/"+corruption, func(t *testing.T) {
				probe := newServingMatrixProbe(servingMatrixClaim, 1)
				workers := 1
				f := newServingMatrixFixture(t, backend, &workers, probe)
				logger := installServingD3Logger(t, f, 0)
				ctx, runID := f.startRun(t, 0)
				trigger := f.submit(t, 0, runID, "d3-corrupt-header")
				turn := waitServingMatrixHeld(t, probe)
				defer turn.release()
				waitServingMatrixTriggerReceipt(t, f.db, trigger)
				before := readServingLifetimeState(t, f.db, runID)
				assertServingLifetimeUnissued(t, before)
				restore := injectServingD3HeaderCorruption(t, f, backend, runID, corruption)
				defer restore()
				page := fanoutobligation.ListPage{RunID: runID, Intents: []fanoutobligation.IntentReadback{{Key: turn.key, BundleHash: f.runtimes[0].sourceArtifactFact.BundleHash()}}}
				if observed, err := startupownership.ObserveFanOutRuntimePage(ctx, f.topology.capability, page); err == nil {
					t.Errorf("corrupt canonical header became runtime evidence: %+v", observed)
				}
				assertServingD3Quiet(t, logger)
				assertNoServingD3Report(t, probe)
				select {
				case attempt := <-probe.attempts:
					t.Fatalf("corrupt-header diagnostic entered actual claim: %+v", attempt)
				default:
				}
				reader := f.selected.(servingD3Reader)
				logs, err := reader.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{Component: "workflow-runtime", Level: "warn", Limit: 100})
				if err != nil || logs.NextCursor != "" {
					t.Fatalf("corrupt-header log read incomplete: %+v err=%v", logs, err)
				}
				for _, log := range logs.Logs {
					if log.ErrorCode == servingD3Code {
						t.Fatalf("corrupt header produced a positive D3 log: %+v", log)
					}
				}
				incidents, err := reader.ListOperatorRuntimeIncidents(ctx, operatorread.OperatorRuntimeIncidentListOptions{SinceHours: 1, Component: "workflow-runtime", Level: "warn", Limit: 100})
				if err != nil || incidents.NextCursor != "" {
					t.Fatalf("corrupt-header incident read incomplete: %+v err=%v", incidents, err)
				}
				for _, incident := range incidents.Incidents {
					if incident.ErrorCode == servingD3Code {
						t.Fatalf("corrupt header produced a positive D3 incident: %+v", incident)
					}
				}
				after := readServingLifetimeState(t, f.db, runID)
				if after.cursor != before.cursor || after.outcomes != before.outcomes || after.history != before.history || after.generation != before.generation {
					t.Fatalf("corrupt-header observation changed issuance: before=%+v after=%+v", before, after)
				}
				restore()
				turn.release()
				f.assertSettled(t, 0, runID, 1)
				t.Logf("M09/M13/%s/%s: canonical malformed-header refusal reached the real detector; no D3 report/log/incident or issuance; restored work settled", backend, corruption)
			})
		}
	}
}

func injectServingD3HeaderCorruption(t *testing.T, f *servingMatrixFixture, backend, runID, corruption string) func() {
	t.Helper()
	ctx := context.Background()
	update := `UPDATE fan_out_intents SET retry_ready_at=$2,retry_failure='{}' WHERE run_id=$1`
	if corruption == "orphan_lease" {
		update = `UPDATE fan_out_intents SET claim_owner=NULL,lease_expires_at=$2 WHERE run_id=$1`
	}
	args := []any{runID, time.Now().UTC().Add(-time.Minute)}
	_, updateErr := f.db.ExecContext(ctx, update, args...)
	var constraint string
	if corruption == "orphan_lease" {
		if updateErr == nil {
			t.Fatal("DDL accepted an orphan lease instead of requiring isolated corruption injection")
		}
		// Reuse the store agent's proven corruption, bypassing constraints only
		// inside this ephemeral fixture. Restore both data and DDL before release.
		if backend == "postgres" {
			if err := f.db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='fan_out_intents'::regclass AND conname='fan_out_intents_check2'`).Scan(&constraint); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.ExecContext(ctx, `ALTER TABLE fan_out_intents DROP CONSTRAINT fan_out_intents_check2`); err != nil {
				t.Fatal(err)
			}
			_, updateErr = f.db.ExecContext(ctx, update, args...)
		} else {
			conn, err := f.db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = conn.ExecContext(ctx, `PRAGMA ignore_check_constraints=ON`); err == nil {
				_, updateErr = conn.ExecContext(ctx, update, args...)
			}
			_, resetErr := conn.ExecContext(ctx, `PRAGMA ignore_check_constraints=OFF`)
			closeErr := conn.Close()
			if err != nil || resetErr != nil || closeErr != nil {
				t.Fatalf("isolated corruption connection: %v %v %v", err, resetErr, closeErr)
			}
		}
	}
	if updateErr != nil {
		t.Fatal(updateErr)
	}
	restored := false
	return func() {
		if restored {
			return
		}
		if _, err := f.db.ExecContext(ctx, `UPDATE fan_out_intents SET claim_owner=NULL,lease_expires_at=NULL,retry_ready_at=NULL,retry_failure=NULL WHERE run_id=$1`, runID); err != nil {
			t.Error(err)
			return
		}
		if constraint != "" {
			if _, err := f.db.ExecContext(ctx, `ALTER TABLE fan_out_intents ADD CONSTRAINT fan_out_intents_check2 `+constraint); err != nil {
				t.Error(err)
				return
			}
		}
		restored = true
	}
}

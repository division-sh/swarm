package serveapp

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/apiv1"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func corruptPublicationAuditResult(result runtimebus.CommittedAPIEventPublication, err error, fault string) (runtimebus.CommittedAPIEventPublication, error) {
	if !result.Acknowledged || result.Replay || fault == "" {
		return result, err
	}
	switch fault {
	case "completion":
		result.Completion = apiidempotency.Completion{}
	case "publication":
		result.Publication.AppendOutcome = runtimebus.EventAppendOutcome(255)
	}
	return result, errors.Join(err, errors.New("post-ACK publication projection fault"), result.Validate())
}

func (p *publicationCompletionAudit) corruptDeployment(result runtimebus.CommittedDeploymentRunCreation, err error) (runtimebus.CommittedDeploymentRunCreation, error) {
	p.mu.Lock()
	fault := p.resultFault
	p.mu.Unlock()
	if fault == "deployment" && result.Acknowledged && !result.Replay {
		result.Record.Summary.Outcome = "invalid"
		err = errors.Join(err, errors.New("post-ACK deployment projection fault"))
	}
	return result, err
}

func TestPublicFeedOnlyInvalidAcknowledgedResultBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			probe := installPublicationCompletionAudit(t, true)
			probe.resultFault = "deployment"
			defer probe.releaseOnce.Do(func() { close(probe.release) })
			h := newChannelOnboardingE2EHarness(t, backend, false)
			h.opts.SourceRoot = canonicalrouting.CopyPublicCompletionFeedOnlyActivation(t)
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			var identity apiv1.RuntimeIdentityResult
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "runtime.identity", map[string]any{}, &identity)
			if len(identity.SourceArtifacts) != 1 {
				t.Fatalf("initial source selection: %+v", identity)
			}
			params := map[string]any{
				"bundle_hash": identity.SourceArtifacts[0].BundleHash, "run_id": uuid.NewString(), "idempotency_key": uuid.NewString(),
				"data": map[string]any{"imports": []any{map[string]any{
					"source_invocation_id": uuid.NewString(), "declaration": map[string]any{"flow_path": ".", "event": "scan.requested"},
					"expected_head": map[string]any{"state": "absent"}, "input": map[string]any{"format": "jsonl", "content_base64": ""},
				}}, "pins": []any{}},
			}
			first := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), "run.start", params)
			if first.err != nil || len(first.rpcError) == 0 || len(first.result) != 0 {
				t.Fatalf("invalid deployment result was not refused: error=%v rpc=%s result=%s", first.err, first.rpcError, first.result)
			}
			select {
			case <-probe.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("acknowledged deployment lost its activation responsibility")
			}
			probe.mu.Lock()
			process, native, request, committed, plan, workflow := probe.process, probe.native, probe.request, probe.deployment, probe.plan, probe.workflow
			probe.mu.Unlock()
			if committed == nil || !committed.Acknowledged || process.PublicCompletionCount() != 1 {
				t.Fatal("invalid returned summary disposed acknowledged deployment early")
			}
			stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
			if err != nil || !found || !bytes.Equal(stored.Response, committed.Completion.Response) {
				t.Fatalf("deployment fault altered original completion: found=%t err=%v", found, err)
			}
			replay := publicationCompletionHTTPMethod(t.Context(), h.rpcEndpoint(), "run.start", params)
			if replay.err != nil || len(replay.rpcError) != 0 || !bytes.Equal(replay.result, stored.Response) {
				t.Fatalf("held deployment replay changed native outcome: %+v", replay)
			}
			probe.releaseOnce.Do(func() { close(probe.release) })
			deadline := time.Now().Add(20 * time.Second)
			for {
				readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(t.Context(), plan.RunID, plan.Identity.Route())
				if err != nil || !found {
					t.Fatalf("original deployment readiness: found=%t err=%v", found, err)
				}
				if readiness.Phase == pipeline.FlowAttachmentReady && process.PublicCompletionCount() == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("deployment did not finish its exact activation: %+v", readiness)
				}
				time.Sleep(20 * time.Millisecond)
			}
			h.stop(t)
			if process.PublicCompletionCount() != 0 {
				t.Fatal("deployment shutdown retained its slot")
			}
		})
	}
}

type publicationAuditPipelineRelease struct {
	pipelineobligation.Store
	pipelineobligation.ResumeAdmissionPreflighter
	probe *publicationCompletionAudit
}

func (s *publicationAuditPipelineRelease) Release(ctx context.Context, claim pipelineobligation.Claim) error {
	s.probe.mu.Lock()
	hold := s.probe.holdRelease && s.probe.committed.Acknowledged && claim.EventID() == s.probe.committed.Completion.ResourceID
	s.probe.mu.Unlock()
	if hold {
		s.probe.enterOnce.Do(func() { close(s.probe.entered) })
		select {
		case <-s.probe.release:
		case <-ctx.Done():
		}
	}
	err := s.Store.Release(ctx, claim)
	s.recordClaimDisposition(claim, err)
	return err
}

func (s *publicationAuditPipelineRelease) Settle(ctx context.Context, claim pipelineobligation.Claim, disposition pipelineobligation.Disposition) (pipelineobligation.SettlementOutcome, error) {
	outcome, err := s.Store.Settle(ctx, claim, disposition)
	if outcome.Committed() {
		s.recordClaimDisposition(claim, err)
	}
	return outcome, err
}

func (s *publicationAuditPipelineRelease) recordClaimDisposition(claim pipelineobligation.Claim, err error) {
	s.probe.mu.Lock()
	if claim.EventID() == s.probe.eventID {
		s.probe.claimDisposeCount++
		s.probe.claimDisposeError = err
		s.probe.claimDisposeOnce.Do(func() { close(s.probe.claimDisposed) })
	}
	s.probe.mu.Unlock()
}

func TestPublicPublicationInvalidAcknowledgedResultBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, fault := range []string{"completion", "publication"} {
			t.Run(string(backend)+"/"+fault, func(t *testing.T) {
				probe := installPublicationCompletionAudit(t, fault == "completion")
				probe.resultFault, probe.holdRelease = fault, fault == "publication"
				defer probe.releaseOnce.Do(func() { close(probe.release) })
				h, hash := startChannelAnchorJourneyRuntime(t, backend, "invalid-publication-result-test", false)
				params := publicationAuditParams(hash, "invalid-acknowledged-result")
				first := publicationAuditHTTP(t.Context(), h.rpcEndpoint(), params)
				if first.err != nil || len(first.rpcError) == 0 || len(first.result) != 0 {
					t.Fatalf("invalid acknowledged result was not a prompt fail-closed response: error=%v rpc=%s result=%s", first.err, first.rpcError, first.result)
				}
				select {
				case <-probe.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("acknowledged invalid projection dropped its joined consequence")
				}
				probe.mu.Lock()
				process, native, request, committed, run := probe.process, probe.native, probe.request, probe.committed, probe.run
				probe.mu.Unlock()
				if !committed.Acknowledged || process.PublicCompletionCount() != 1 {
					t.Fatalf("invalid projection lost real ACK responsibility: ack=%t capacity=%d", committed.Acknowledged, process.PublicCompletionCount())
				}
				stored, found, err := native.LookupAPIEventPublication(t.Context(), request)
				if err != nil || !found || !bytes.Equal(stored.Response, committed.Completion.Response) {
					t.Fatalf("projection fault altered native completion: found=%t err=%v", found, err)
				}
				evidence := storetest.ReadSemanticEventFixtureEvidence(t, t.Context(), native, run, committed.Completion.ResourceID)
				if !evidence.RecordFound || !evidence.CommittedScopeFound || evidence.PipelineReceiptOutcome == "success" {
					t.Fatalf("held fault lost committed recovery evidence or settled early: %+v", evidence)
				}
				probe.releaseOnce.Do(func() { close(probe.release) })
				if fault == "completion" {
					waitPublicationCompletionDisposition(t, probe)
				}
				h.stop(t)
				if process.PublicCompletionCount() != 0 {
					t.Fatal("shutdown did not join invalid-result cleanup")
				}
				probe.mu.Lock()
				probe.resultFault, probe.hold, probe.holdRelease = "", false, false
				if probe.plan.RunID == "" && len(committed.Publication.Activations) != 0 {
					probe.plan = committed.Publication.Activations[0].Plan.Readiness
				}
				probe.mu.Unlock()
				h.start(t)
				waitPublicationCompletionDisposition(t, probe)
				replay := publicationAuditHTTP(t.Context(), h.rpcEndpoint(), params)
				probe.mu.Lock()
				calls := probe.commits
				probe.mu.Unlock()
				if replay.err != nil || len(replay.rpcError) != 0 || !bytes.Equal(replay.result, stored.Response) || calls != 1 {
					t.Fatalf("restart replay reconstructed or reexecuted the outcome: error=%v rpc=%s commits=%d", replay.err, replay.rpcError, calls)
				}
			})
		}
	}
}

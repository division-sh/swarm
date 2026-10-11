package serveapp

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
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
	return s.Store.Release(ctx, claim)
}

func TestPublicPublicationInvalidAcknowledgedResultBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, fault := range []string{"completion", "publication"} {
			t.Run(string(backend)+"/"+fault, func(t *testing.T) {
				probe := installPublicationCompletionAudit(t, fault == "completion")
				probe.resultFault, probe.holdRelease = fault, fault == "publication"
				defer probe.releaseOnce.Do(func() { close(probe.release) })
				h, _, hash := startChannelAnchorJourney(t, backend, "invalid-publication-result-test", false)
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

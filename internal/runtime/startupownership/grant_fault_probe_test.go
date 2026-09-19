package startupownership

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type wrappedFaultProbeGrant struct{ LiveGenerationGrant }

func TestGenerationGrantFaultProbePreservesNativeAuthority(t *testing.T) {
	capability, session, plan := testCapability(t)
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	issue := func(generation uint64) LiveGenerationGrant {
		grant, err := capability.IssueGenerationGrant(context.Background(), GrantRequest{
			BundleHash: startupBundleHashA, RuntimeInstanceID: session.authority.RuntimeInstanceID,
			RuntimeGeneration: generation, SourceSetRevision: plan.Revision,
		})
		if err != nil {
			t.Fatal(err)
		}
		return grant
	}
	grant, sibling := issue(1), issue(2)
	before, err := grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	var evidenceErr, settleErr, retireErr error
	retireObserved := 0
	probe := GenerationGrantFaultProbeForTest{
		EvidenceError:    func() error { return evidenceErr },
		BeforeSettlement: func() error { return settleErr },
		BeforeRetirement: func() {
			retireObserved++
			if retireObserved == 1 {
				select {
				case <-grant.Done():
					t.Error("retirement observation ran after native retirement")
				default:
				}
			}
		},
		AfterRetirementError: func() error {
			select {
			case <-grant.Done():
			default:
				t.Error("post-retirement fault ran before native retirement")
			}
			return retireErr
		},
	}
	var typedNil *liveGenerationGrant
	for _, invalid := range []LiveGenerationGrant{nil, typedNil, &wrappedFaultProbeGrant{grant}} {
		if restore, err := InstallGenerationGrantFaultProbeForTest(invalid, probe); err == nil || restore != nil {
			t.Fatal("non-native grant accepted a fault probe")
		}
	}
	restore, err := InstallGenerationGrantFaultProbeForTest(grant, probe)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restore)
	if again, err := InstallGenerationGrantFaultProbeForTest(grant, probe); err == nil || again != nil {
		t.Fatal("duplicate probe installation succeeded")
	}
	cause := errors.New("exact fixture refusal")
	evidenceErr = cause
	if _, err := grant.Evidence(); !errors.Is(err, cause) {
		t.Fatalf("evidence refusal identity: %v", err)
	}
	if _, err := sibling.Evidence(); err != nil {
		t.Fatalf("probe affected sibling: %v", err)
	}
	evidenceErr = nil
	settleErr = cause
	if _, err := grant.MarkProbesSettled(context.Background(), nil); !errors.Is(err, cause) {
		t.Fatalf("settlement refusal identity: %v", err)
	}
	after, err := grant.Evidence()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("refused calls changed native evidence: before=%+v after=%+v err=%v", before, after, err)
	}
	session.mu.Lock()
	records := len(session.records)
	session.mu.Unlock()
	if records != 2 {
		t.Fatalf("refused calls wrote grant records: %d", records)
	}
	settleErr = nil
	if _, err := grant.MarkProbesSettled(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(context.Background()); err != nil {
		t.Fatal(err)
	}
	retireErr = cause
	if err := grant.Retire(context.Background()); !errors.Is(err, cause) {
		t.Fatalf("post-retirement error identity: %v", err)
	}
	session.mu.Lock()
	last := session.records[len(session.records)-1]
	session.mu.Unlock()
	if last.GrantID != before.GrantID || last.State != GrantRetired || retireObserved != 1 {
		t.Fatalf("native retirement not preserved: record=%+v observations=%d", last, retireObserved)
	}
	if _, err := sibling.Evidence(); err != nil {
		t.Fatalf("retirement fault affected sibling: %v", err)
	}
	if err := grant.Retire(context.Background()); !errors.Is(err, cause) || retireObserved != 2 {
		t.Fatalf("duplicate retirement lost original fixture behavior: err=%v observations=%d", err, retireObserved)
	}
	restore()
	restore()
	if err := grant.Retire(context.Background()); err != nil || retireObserved != 2 {
		t.Fatalf("restoration retained probe: err=%v observations=%d", err, retireObserved)
	}
	if _, err := grant.Evidence(); err == nil {
		t.Fatal("restoration revived a retired native grant")
	}
	if _, err := InstallGenerationGrantFaultProbeForTest(grant, probe); err == nil {
		t.Fatal("retired grant accepted a probe")
	}
	if _, err := grant.MarkProbesSettled(context.Background(), nil); err == nil {
		t.Fatal("probe restoration bypassed native transition refusal")
	}
}

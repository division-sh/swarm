package startupownership

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAgentRecoverySeedCheckRequiresExactPrivateBinding(t *testing.T) {
	for _, fault := range []string{"absent", "source", "counts", "lease", "generation", "owner", "boot", "runtime", "backend", "observer", "observer_process", "invalid"} {
		t.Run(fault, func(t *testing.T) {
			p, session, _ := newAgentRecoverySeedCapability(t)
			if err := SeedAgentRecovery(t.Context(), p); err != nil {
				t.Fatal(err)
			}
			writes := session.writes
			writes.mu.Lock()
			p.mu.Lock()
			switch fault {
			case "absent":
				p.agentRecoverySeed = nil
			case "source":
				p.agentRecoverySeed.sourceHead = ""
			case "counts":
				p.agentRecoverySeed.inProgressByProfile = nil
			case "lease":
				p.agentRecoverySeed.process.AuthorityID = uuid.NewString()
			case "generation":
				p.agentRecoverySeed.process.AuthorityGeneration++
			case "owner":
				p.agentRecoverySeed.process.OwnerID = "foreign"
			case "boot":
				p.agentRecoverySeed.process.BootID = uuid.NewString()
			case "runtime":
				p.agentRecoverySeed.process.RuntimeInstanceID = uuid.NewString()
			case "backend":
				p.agentRecoverySeed.process.Backend = "foreign"
			case "observer":
				p.agentRecoveryWrites = &AgentRecoveryWriteObserver{process: p}
			case "observer_process":
				writes.process = nil
			case "invalid":
				writes.invalid = true
			}
			p.mu.Unlock()
			writes.mu.Unlock()
			var proofs atomic.Int32
			session.monitorProve = func(context.Context, time.Duration) error { proofs.Add(1); return nil }
			if err := CheckAgentRecoverySeed(t.Context(), p); !errors.Is(err, errAgentRecoverySeedChanged) || proofs.Load() != 0 {
				t.Fatalf("private binding refusal changed native proof ownership: %v proofs=%d", err, proofs.Load())
			}
			if _, terminal := p.TerminalResult(); terminal {
				t.Fatal("private seed/domain refusal terminalized healthy native possession")
			}
			if err := p.ProveCurrent(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAgentRecoverySeedCheckDoesNotReadOrReseed(t *testing.T) {
	p, session, read := newAgentRecoverySeedCapability(t)
	var reads, proofs atomic.Int32
	session.read = func(context.Context) (AgentRecoverySeedRead, error) { reads.Add(1); return read, nil }
	session.monitorProve = func(context.Context, time.Duration) error { proofs.Add(1); return nil }
	if err := CheckAgentRecoverySeed(t.Context(), p); err == nil || reads.Load() != 0 || proofs.Load() != 0 {
		t.Fatalf("absent seed performed native work: %v reads=%d proofs=%d", err, reads.Load(), proofs.Load())
	}
	if err := SeedAgentRecovery(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	original := p.agentRecoverySeed
	for range 2 {
		if err := CheckAgentRecoverySeed(t.Context(), p); err != nil || p.agentRecoverySeed != original || reads.Load() != 1 {
			t.Fatalf("read-only check replaced/reconstructed seed: %v reads=%d", err, reads.Load())
		}
	}
	var nilCapability *processCapability
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, input := range []struct {
		ctx context.Context
		cap ProcessCapability
	}{{nil, p}, {t.Context(), nil}, {t.Context(), nilCapability}, {canceled, p}, {t.Context(), struct{ ProcessCapability }{p}}} {
		if err := CheckAgentRecoverySeed(input.ctx, input.cap); err == nil || reads.Load() != 1 || proofs.Load() != 2 {
			t.Fatalf("substituted caller performed work: %v reads=%d proofs=%d", err, reads.Load(), proofs.Load())
		}
	}
	write := session.writes.ObserveWriteStart()
	if err := CheckAgentRecoverySeed(t.Context(), p); !errors.Is(err, errAgentRecoverySeedChanged) {
		t.Fatalf("active writer did not refuse: %v", err)
	}
	write.End()
	if err := CheckAgentRecoverySeed(t.Context(), p); !errors.Is(err, errAgentRecoverySeedChanged) || reads.Load() != 1 || proofs.Load() != 2 {
		t.Fatalf("joined writer implicitly reseeded: %v reads=%d proofs=%d", err, reads.Load(), proofs.Load())
	}
	if err := SeedAgentRecovery(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	if err := CheckAgentRecoverySeed(t.Context(), p); err != nil || reads.Load() != 2 || proofs.Load() != 3 || p.agentRecoverySeed == original {
		t.Fatalf("explicit complete reseed lost original ownership: %v reads=%d proofs=%d", err, reads.Load(), proofs.Load())
	}
}

func TestAgentRecoverySeedCheckRechecksAcrossProof(t *testing.T) {
	for _, cut := range []string{"capture", "proof"} {
		for _, change := range []string{"write", "reseed", "same_value_replacement", "epoch", "source", "stamp", "cancel", "release"} {
			t.Run(cut+"/"+change, func(t *testing.T) {
				p, session, read := newAgentRecoverySeedCapability(t)
				if err := SeedAgentRecovery(t.Context(), p); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				observe := func() {
					if !p.opMu.TryLock() {
						t.Fatal("checker held process-operation lock across passive cut")
					}
					p.opMu.Unlock()
					if !session.writes.mu.TryLock() {
						t.Fatal("checker held observer lock across passive cut")
					}
					session.writes.mu.Unlock()
					if !p.mu.TryLock() {
						t.Fatal("checker held process lock across passive cut")
					}
					p.mu.Unlock()
					if err := ConsumeAgentRecoverySeedFaultCutForTest(ctx, p, read.Process); err == nil || p.agentRecoverySeedFaultUsed.Load() {
						t.Fatal("passive check notification authorized native seed interruption")
					}
					switch change {
					case "write", "reseed":
						write := session.writes.ObserveWriteStart()
						write.End()
						if change == "reseed" {
							if err := SeedAgentRecovery(t.Context(), p); err != nil {
								t.Fatal(err)
							}
						}
					case "same_value_replacement":
						p.mu.Lock()
						replacement := *p.agentRecoverySeed
						p.agentRecoverySeed = &replacement
						p.mu.Unlock()
					case "epoch":
						session.writes.mu.Lock()
						session.writes.epoch++
						session.writes.mu.Unlock()
					case "source":
						p.mu.Lock()
						p.agentRecoverySeed.sourceHead = "foreign source binding"
						p.mu.Unlock()
					case "stamp":
						p.mu.Lock()
						p.agentRecoverySeed.process.OwnerID = "foreign"
						p.mu.Unlock()
					case "cancel":
						cancel()
					case "release":
						if err := p.Release(t.Context()); err != nil {
							t.Fatal(err)
						}
					}
				}
				probe := &AgentRecoverySeedFaultProbeForTest{}
				if cut == "capture" {
					probe.AfterCheckCapture = observe
				} else {
					probe.AfterCheckProof = observe
				}
				remove, err := InstallAgentRecoverySeedFaultProbeForTest(p, probe)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(remove)
				err = CheckAgentRecoverySeed(ctx, p)
				if err == nil || change == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("late check accepted %s or lost cancellation: %v", change, err)
				}
				_, terminal := p.TerminalResult()
				if terminal != (change == "release") {
					t.Fatalf("seed/context refusal changed native terminality: terminal=%t", terminal)
				}
			})
		}
	}
}

func TestAgentRecoverySeedCheckPreservesNativeProofFailure(t *testing.T) {
	p, session, _ := newAgentRecoverySeedCapability(t)
	if err := SeedAgentRecovery(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("original retained native proof failed")
	session.monitorProve = func(context.Context, time.Duration) error { return failure }
	if err := CheckAgentRecoverySeed(t.Context(), p); !errors.Is(err, failure) || p.agentRecoverySeed != nil {
		t.Fatalf("check masked native loss or retained its seed: %v", err)
	}
	if _, terminal := p.TerminalResult(); !terminal {
		t.Fatal("native proof loss did not terminalize original process")
	}
}

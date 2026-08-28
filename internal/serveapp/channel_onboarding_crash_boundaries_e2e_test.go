package serveapp

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelOnboardingE2E13CredentialWriteBeforeCheckpoint(t *testing.T) {
	runChannelOnboardingCrashBoundaryE2E(t, channelonboarding.TestAfterCredentialWriteBeforeCheckpoint)
}

func TestChannelOnboardingE2E14ActivationCommitBeforeProcessPublication(t *testing.T) {
	runChannelOnboardingCrashBoundaryE2E(t, channelonboarding.TestAfterActivationCommitBeforePublication)
}

func TestChannelOnboardingE2E16ProcessPublicationBeforePromotion(t *testing.T) {
	runChannelOnboardingCrashBoundaryE2E(t, channelonboarding.TestAfterProcessPublicationBeforePromotion)
}

func TestChannelOnboardingE2E17AuthorityRetirementBeforeCleanup(t *testing.T) {
	runChannelOnboardingCrashBoundaryE2E(t, channelonboarding.TestAfterAuthorityRetirementBeforeCleanup)
}

func runChannelOnboardingCrashBoundaryE2E(t *testing.T, boundary channelonboarding.TestLifecycleBoundary) {
	t.Helper()
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			harness := newChannelOnboardingE2EHarness(t, backend, true)
			barrier := newChannelOnboardingTestBarrier(boundary)
			if boundary == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint || boundary == channelonboarding.TestAfterProcessPublicationBeforePromotion {
				barrier.Disarm()
			}
			harness.opts.TestChannelOnboardingBarrier = barrier.Reach
			harness.start(t)

			if boundary == channelonboarding.TestAfterAuthorityRetirementBeforeCleanup {
				runChannelOnboardingDestructiveCrashBoundary(t, backend, harness, barrier)
				return
			}
			var predecessor channelOnboardingJourneyReadback
			predecessorCredentialCount := 0
			var command channelOnboardingCLICommand
			if boundary == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint {
				predecessor = runChannelOnboardingCLIJourney(t, harness.opts.ConfigPath, harness.endpoint, harness.provider, "connect", "crash-boundary-token", 7213, "private", 0)
				if predecessor.Activation == nil || predecessor.Readiness == nil || !predecessor.Readiness.Ready {
					t.Fatalf("%s E2E-13 predecessor = %#v", backend, predecessor)
				}
				predecessorCredentialCount = channelOnboardingCredentialCount(t, harness.credentialPath)
				barrier.Arm()
				command = startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, []string{"channel", "reconnect", "telegram", "--yes", "--credential-stdin"}, "crash-boundary-replacement-token\n")
			} else if boundary == channelonboarding.TestAfterProcessPublicationBeforePromotion {
				predecessor = runChannelOnboardingCLIJourney(t, harness.opts.ConfigPath, harness.endpoint, harness.provider, "connect", "crash-boundary-token", 7216, "private", 0)
				if predecessor.Activation == nil || predecessor.Readiness == nil || !predecessor.Readiness.Ready {
					t.Fatalf("%s E2E-16 predecessor = %#v", backend, predecessor)
				}
				barrier.Arm()
				command = startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, []string{"channel", "reconnect", "telegram", "--yes"}, "")
			} else {
				command = startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, []string{"channel", "connect", "telegram", "--yes"}, "crash-boundary-token\n")
			}
			if boundary != channelonboarding.TestAfterCredentialWriteBeforeCheckpoint && boundary != channelonboarding.TestAfterProcessPublicationBeforePromotion {
				challenge := waitChannelOnboardingChallenge(t, command.stdout, command.stderr, command.done)
				callbackURL, signingSecret := waitChannelOnboardingRegistration(t, harness.provider, command.stdout, command.stderr, command.done)
				admission := submitChannelOnboardingClaim(t, callbackURL, signingSecret, challenge, 7200+int64(len(boundary)), "crash_boundary")
				requireChannelClaimDisposition(t, string(boundary), admission, "consumed_by_binding")
			}
			responsibilityID := barrier.Wait(t)
			before := getChannelOnboardingRPC(t, harness, responsibilityID)
			var beforeRow channelOnboardingJourneyReadback
			switch boundary {
			case channelonboarding.TestAfterCredentialWriteBeforeCheckpoint:
				if before.Operation.Phase != channelonboarding.PhasePreparing || len(before.Operation.CredentialAdmissions) != 0 {
					t.Fatalf("%s E2E-13 predecessor checkpoint = %#v", backend, before.Operation)
				}
				if count := channelOnboardingCredentialCount(t, harness.credentialPath); count <= predecessorCredentialCount {
					t.Fatalf("%s E2E-13 physical credential count=%d, want more than predecessor count %d", backend, count, predecessorCredentialCount)
				}
			case channelonboarding.TestAfterActivationCommitBeforePublication:
				if before.Operation.Phase != channelonboarding.PhasePublishingProcessActivation || before.Operation.ActivationRevision < 1 {
					t.Fatalf("%s E2E-14 predecessor checkpoint = %#v", backend, before.Operation)
				}
			case channelonboarding.TestAfterProcessPublicationBeforePromotion:
				if before.Operation.Phase != channelonboarding.PhasePublishingProcessActivation || before.Operation.ActivationRevision < 1 {
					t.Fatalf("%s E2E-16 predecessor checkpoint = %#v", backend, before.Operation)
				}
				beforeRow = requireChannelOnboardingOperationRow(t, harness, responsibilityID)
				if predecessor.Activation == nil || beforeRow.Activation == nil || beforeRow.Activation.Revision <= predecessor.Activation.Revision ||
					beforeRow.Identity.BindingRevision != predecessor.Identity.BindingRevision || beforeRow.Identity.ConversationRef != predecessor.Identity.ConversationRef {
					t.Fatalf("%s E2E-16 successor handoff changed identity or did not advance activation: predecessor=%#v successor=%#v", backend, predecessor, beforeRow)
				}
				if beforeRow.Readiness == nil || beforeRow.Readiness.Ready {
					t.Fatalf("%s E2E-16 reported READY before registration promotion: %#v", backend, beforeRow.Readiness)
				}
				if registrations, deliveries := harness.provider.Counts(); registrations != 1 || deliveries != 1 {
					t.Fatalf("%s E2E-16 effects before promotion = %d/%d, want predecessor 1/1", backend, registrations, deliveries)
				}
			}

			crashChannelOnboardingServeAtBarrier(t, harness, barrier, command)
			harness.opts.TestChannelOnboardingBarrier = nil
			harness.start(t)
			after := getChannelOnboardingRPC(t, harness, responsibilityID)
			if after.Operation.OperationID != before.Operation.OperationID || after.Operation.Coordinate.RuntimeInstanceID == before.Operation.Coordinate.RuntimeInstanceID {
				t.Fatalf("%s %s restart identity = before %#v after %#v", backend, boundary, before.Operation, after.Operation)
			}

			if boundary == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint {
				if len(after.Operation.CredentialAdmissions) != len(after.Operation.CredentialReservations) || after.Operation.Phase != channelonboarding.PhaseSucceeded {
					t.Fatalf("%s E2E-13 rediscovered checkpoint = %#v", backend, after.Operation)
				}
				afterRow := requireChannelOnboardingOperationRow(t, harness, responsibilityID)
				assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-13 reconnect", predecessor, afterRow)
				if afterRow.Activation == nil || predecessor.Activation == nil || afterRow.Activation.Revision <= predecessor.Activation.Revision {
					t.Fatalf("%s E2E-13 recovered reconnect did not advance activation: predecessor=%#v successor=%#v", backend, predecessor, afterRow)
				}
			}
			if after.Operation.Phase != channelonboarding.PhaseSucceeded || after.Readiness == nil || !after.Readiness.Ready {
				t.Fatalf("%s %s recovered result = %#v", backend, boundary, after)
			}
			wantRegistrations, wantDeliveries := 1, 1
			if boundary == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint {
				wantRegistrations, wantDeliveries = 3, 2
			}
			if boundary == channelonboarding.TestAfterActivationCommitBeforePublication {
				wantRegistrations = 2
			}
			if boundary == channelonboarding.TestAfterProcessPublicationBeforePromotion {
				wantRegistrations, wantDeliveries = 2, 2
				afterRow := requireChannelOnboardingOperationRow(t, harness, responsibilityID)
				assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-16 restart", predecessor, afterRow)
				if afterRow.Activation == nil || predecessor.Activation == nil || afterRow.Activation.Revision <= predecessor.Activation.Revision ||
					afterRow.Readiness == nil || afterRow.Readiness.ActivationGeneration == predecessor.Readiness.ActivationGeneration {
					t.Fatalf("%s E2E-16 successor did not replace predecessor: predecessor=%#v successor=%#v", backend, predecessor, afterRow)
				}
			}
			if registrations, deliveries := harness.provider.Counts(); registrations != wantRegistrations || deliveries != wantDeliveries {
				t.Fatalf("%s %s provider effects = %d/%d, want %d/%d", backend, boundary, registrations, deliveries, wantRegistrations, wantDeliveries)
			}
			harness.stop(t)
		})
	}
}

func runChannelOnboardingDestructiveCrashBoundary(t *testing.T, backend servedparity.Backend, harness *channelOnboardingE2EHarness, barrier *channelOnboardingTestBarrier) {
	t.Helper()
	ready := runChannelOnboardingCLIJourney(t, harness.opts.ConfigPath, harness.endpoint, harness.provider, "connect", "destructive-token", 7217, "private", 0)
	if ready.Activation == nil || ready.Readiness == nil || !ready.Readiness.Ready {
		t.Fatalf("%s E2E-17 prerequisite channel = %#v", backend, ready)
	}
	command := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, []string{"channel", "unbind", ready.Identity.Interface.Selector}, "")
	teardownID := barrier.Wait(t)
	if teardownID == "" {
		t.Fatal("E2E-17 teardown barrier lacks responsibility ID")
	}
	assertChannelOnboardingCredentialStoreNotEmpty(t, harness.credentialPath, string(backend)+" E2E-17 before cleanup")
	crashChannelOnboardingServeAtBarrier(t, harness, barrier, command)
	harness.opts.TestChannelOnboardingBarrier = nil
	harness.start(t)
	unbound := readChannelOnboardingRow(t, harness.opts.ConfigPath, harness.endpoint, "unbound")
	if unbound.Activation != nil || unbound.Readiness != nil || unbound.Identity.BindingRevision <= ready.Identity.BindingRevision {
		t.Fatalf("%s E2E-17 recovered teardown = %#v", backend, unbound)
	}
	assertChannelOnboardingCredentialStoreEmpty(t, harness.credentialPath, string(backend)+" E2E-17 recovered cleanup")
	if registrations, deliveries := harness.provider.Counts(); registrations != 1 || deliveries != 1 {
		t.Fatalf("%s E2E-17 replayed provider effects = %d/%d, want 1/1", backend, registrations, deliveries)
	}
	harness.stop(t)
}

type channelOnboardingTestBarrier struct {
	want    channelonboarding.TestLifecycleBoundary
	arrived chan string
	release chan struct{}
	mu      sync.RWMutex
	armed   bool
	once    sync.Once
}

func newChannelOnboardingTestBarrier(want channelonboarding.TestLifecycleBoundary) *channelOnboardingTestBarrier {
	return &channelOnboardingTestBarrier{want: want, arrived: make(chan string, 1), release: make(chan struct{}), armed: true}
}

func (b *channelOnboardingTestBarrier) Arm() {
	b.mu.Lock()
	b.armed = true
	b.mu.Unlock()
}

func (b *channelOnboardingTestBarrier) Disarm() {
	b.mu.Lock()
	b.armed = false
	b.mu.Unlock()
}

func (b *channelOnboardingTestBarrier) Reach(boundary channelonboarding.TestLifecycleBoundary, responsibilityID string) error {
	if b == nil || boundary != b.want {
		return nil
	}
	b.mu.RLock()
	armed := b.armed
	b.mu.RUnlock()
	if !armed {
		return nil
	}
	blocked := false
	b.once.Do(func() {
		blocked = true
		b.arrived <- responsibilityID
		<-b.release
	})
	if blocked {
		return errors.New("injected served-process loss after " + string(boundary))
	}
	return nil
}

func (b *channelOnboardingTestBarrier) Wait(t *testing.T) string {
	t.Helper()
	select {
	case responsibilityID := <-b.arrived:
		return responsibilityID
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for lifecycle boundary %s", b.want)
		return ""
	}
}

func crashChannelOnboardingServeAtBarrier(t *testing.T, harness *channelOnboardingE2EHarness, barrier *channelOnboardingTestBarrier, command channelOnboardingCLICommand) {
	t.Helper()
	harness.process.cancel()
	close(barrier.release)
	select {
	case code := <-command.done:
		if code == 0 {
			t.Fatalf("channel command succeeded across injected process loss\nstdout:\n%s\nstderr:\n%s", command.stdout.String(), command.stderr.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("channel command did not settle after injected process loss\nstdout:\n%s\nstderr:\n%s", command.stdout.String(), command.stderr.String())
	}
	code, exited := harness.process.waitForExit(serveRuntimeStopTimeout)
	if !exited || code != 0 {
		t.Fatalf("serve did not stop cleanly after injected process loss: exited=%v code=%d\n%s", exited, code, harness.process.outputString())
	}
	harness.process = nil
}

func assertChannelOnboardingCredentialStoreNotEmpty(t *testing.T, credentialPath, label string) {
	t.Helper()
	if count := channelOnboardingCredentialCount(t, credentialPath); count == 0 {
		t.Fatalf("%s credential store is empty", label)
	}
}

func channelOnboardingCredentialCount(t *testing.T, credentialPath string) int {
	t.Helper()
	store, err := credentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("open credential store: %v", err)
	}
	keys, err := store.List(t.Context())
	if err != nil {
		t.Fatalf("list credential store: %v", err)
	}
	return len(keys)
}

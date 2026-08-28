package serveapp

import (
	"errors"
	"net/http"
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
			defer barrier.Release()
			t.Cleanup(barrier.Release)
			if boundary == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint || boundary == channelonboarding.TestAfterActivationCommitBeforePublication || boundary == channelonboarding.TestAfterProcessPublicationBeforePromotion {
				barrier.Disarm()
			}
			var uncheckpointedBarrier *channelOnboardingTestBarrier
			if boundary == channelonboarding.TestAfterAuthorityRetirementBeforeCleanup {
				uncheckpointedBarrier = newChannelOnboardingTestBarrier(channelonboarding.TestAfterCredentialWriteBeforeCheckpoint)
				defer uncheckpointedBarrier.Release()
				t.Cleanup(uncheckpointedBarrier.Release)
				uncheckpointedBarrier.Disarm()
				harness.opts.TestChannelOnboardingBarrier = func(observed channelonboarding.TestLifecycleBoundary, responsibilityID string) error {
					if err := uncheckpointedBarrier.Reach(observed, responsibilityID); err != nil {
						return err
					}
					return barrier.Reach(observed, responsibilityID)
				}
			} else {
				harness.opts.TestChannelOnboardingBarrier = barrier.Reach
			}
			harness.start(t)

			if boundary == channelonboarding.TestAfterAuthorityRetirementBeforeCleanup {
				runChannelOnboardingDestructiveCrashBoundary(t, backend, harness, barrier, uncheckpointedBarrier)
				return
			}
			var predecessor channelOnboardingJourneyReadback
			predecessorCredentialCount := 0
			predecessorCallback, predecessorSigning := "", ""
			var recoveredPublication channelOnboardingPersistedHandoff
			var command channelOnboardingCLICommand
			if boundary == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint {
				predecessor = runChannelOnboardingCLIJourney(t, harness.opts.ConfigPath, harness.endpoint, harness.provider, "connect", "crash-boundary-token", 7213, "private", 0)
				if predecessor.Activation == nil || predecessor.Readiness == nil || !predecessor.Readiness.Ready {
					t.Fatalf("%s E2E-13 predecessor = %#v", backend, predecessor)
				}
				predecessorCredentialCount = channelOnboardingCredentialCount(t, harness.credentialPath)
				barrier.Arm()
				command = startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, []string{"channel", "reconnect", "telegram", "--yes", "--credential-stdin"}, "crash-boundary-replacement-token\n")
			} else if boundary == channelonboarding.TestAfterActivationCommitBeforePublication {
				predecessor = runChannelOnboardingCLIJourney(t, harness.opts.ConfigPath, harness.endpoint, harness.provider, "connect", "crash-boundary-token", 7214, "private", 0)
				if predecessor.Activation == nil || predecessor.Readiness == nil || !predecessor.Readiness.Ready {
					t.Fatalf("%s E2E-14 predecessor = %#v", backend, predecessor)
				}
				predecessorCredentialCount = channelOnboardingCredentialCount(t, harness.credentialPath)
				predecessorCallback, predecessorSigning, _ = harness.provider.Registration()
				harness.provider.SetResourceID("crash-boundary-rebind-token", 420114)
				barrier.Arm()
				command = startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, []string{"channel", "rebind", "telegram", "--yes", "--credential-stdin"}, "crash-boundary-rebind-token\n")
				challenge := waitChannelOnboardingChallenge(t, command.stdout, command.stderr, command.done)
				successorCallback, successorSigning := waitChannelOnboardingRegistrationForCredential(t, harness.provider, "crash-boundary-rebind-token", 2, command)
				claim := submitChannelOnboardingClaimWithChatType(t, successorCallback, successorSigning, challenge, 7214, 8214, -9214, "group", "crash_boundary_rebind")
				requireChannelClaimDisposition(t, "E2E-14 successor claim", claim, "consumed_by_binding")
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
			if boundary != channelonboarding.TestAfterCredentialWriteBeforeCheckpoint && boundary != channelonboarding.TestAfterActivationCommitBeforePublication && boundary != channelonboarding.TestAfterProcessPublicationBeforePromotion {
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
				beforeRow = requireChannelOnboardingOperationRow(t, harness, responsibilityID)
				if beforeRow.Identity.Interface.Selector != predecessor.Identity.Interface.Selector || beforeRow.Identity.BindingRevision <= predecessor.Identity.BindingRevision ||
					beforeRow.Identity.ConversationScope != "shared" || beforeRow.Identity.ConversationRef == predecessor.Identity.ConversationRef {
					t.Fatalf("%s E2E-14 committed rebind did not retain the exact successor identity: predecessor=%#v successor=%#v", backend, predecessor.Identity, beforeRow.Identity)
				}
				if beforeRow.Activation == nil || predecessor.Activation == nil || beforeRow.Activation.Revision <= predecessor.Activation.Revision || beforeRow.Activation.Revision != before.Operation.ActivationRevision {
					t.Fatalf("%s E2E-14 durable activation commit = predecessor %#v committed %#v operation %#v", backend, predecessor.Activation, beforeRow.Activation, before.Operation)
				}
				if beforeRow.Readiness == nil || beforeRow.Readiness.Ready || beforeRow.Readiness.ActivationGeneration != predecessor.Readiness.ActivationGeneration {
					t.Fatalf("%s E2E-14 process authority changed before publication: predecessor=%#v committed=%#v", backend, predecessor.Readiness, beforeRow.Readiness)
				}
				if count := channelOnboardingCredentialCount(t, harness.credentialPath); count <= predecessorCredentialCount {
					t.Fatalf("%s E2E-14 predecessor credentials were cleaned before process publication: count=%d predecessor=%d", backend, count, predecessorCredentialCount)
				}
				if registrations, deliveries := harness.provider.Counts(); registrations != 2 || deliveries != 1 {
					t.Fatalf("%s E2E-14 effects before publication = %d/%d, want staged registrations 2 and predecessor confirmation 1", backend, registrations, deliveries)
				}
				requireChannelClaimDisposition(t, "E2E-14 predecessor callback before publication",
					submitChannelOnboardingClaim(t, predecessorCallback, predecessorSigning, "SWARM-AAAAAAAAAAAAAAAA", 7215, "predecessor_before_publication"), "rejected_binding_claim")
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
			if boundary == channelonboarding.TestAfterActivationCommitBeforePublication {
				recoveredPublication = restartChannelOnboardingAtProcessPublicationBoundary(t, backend, harness, responsibilityID, predecessorCredentialCount, beforeRow)
			} else {
				harness.opts.TestChannelOnboardingBarrier = nil
				harness.start(t)
			}
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
				wantRegistrations, wantDeliveries = 3, 2
				afterRow := requireChannelOnboardingOperationRow(t, harness, responsibilityID)
				assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-14 restart", beforeRow, afterRow)
				if afterRow.Activation == nil || afterRow.Activation.Revision != recoveredPublication.ActivationRevision || afterRow.Operation == nil ||
					afterRow.Operation.Coordinate.RuntimeInstanceID != recoveredPublication.ActivationRuntimeInstanceID ||
					afterRow.Readiness == nil || afterRow.Readiness.ActivationGeneration == predecessor.Readiness.ActivationGeneration {
					t.Fatalf("%s E2E-14 did not retain the exact recovery publication: barrier=%#v recovered=%#v", backend, recoveredPublication, afterRow)
				}
				if count := channelOnboardingCredentialCount(t, harness.credentialPath); count != predecessorCredentialCount {
					t.Fatalf("%s E2E-14 credential handoff count=%d, want exact successor count %d", backend, count, predecessorCredentialCount)
				}
				retired := submitChannelOnboardingClaim(t, predecessorCallback, predecessorSigning, "SWARM-AAAAAAAAAAAAAAAA", 7216, "retired_predecessor")
				if retired.StatusCode != http.StatusNotFound {
					t.Fatalf("%s E2E-14 predecessor callback survived promotion: %#v", backend, retired)
				}
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

func restartChannelOnboardingAtProcessPublicationBoundary(t *testing.T, backend servedparity.Backend, harness *channelOnboardingE2EHarness, operationID string, predecessorCredentialCount int, committed channelOnboardingJourneyReadback) channelOnboardingPersistedHandoff {
	t.Helper()
	publicationBarrier := newChannelOnboardingTestBarrier(channelonboarding.TestAfterProcessPublicationBeforePromotion)
	publicationBarrier.ContinueAfterRelease()
	defer publicationBarrier.Release()
	harness.opts.TestChannelOnboardingBarrier = publicationBarrier.Reach
	apiListen := reserveChannelOnboardingListenAddress(t)
	harness.opts.APIListenAddr = apiListen
	harness.process = startServeRuntimeTestProcess(t, harness.opts)
	harness.endpoint = "http://" + apiListen
	if recoveredID := publicationBarrier.Wait(t); recoveredID != operationID {
		t.Fatalf("%s E2E-14 recovery publication responsibility=%s, want %s", backend, recoveredID, operationID)
	}
	assertChannelOnboardingServeNotReady(t, harness.endpoint)
	publication := channelOnboardingPersistedHandoffAtPublication(t, harness, operationID)
	if committed.Operation == nil || committed.Activation == nil ||
		publication.OperationPhase != string(channelonboarding.PhasePublishingProcessActivation) ||
		publication.OperationActivationRevision != publication.ActivationRevision ||
		publication.ActivationRevision <= committed.Activation.Revision ||
		publication.OperationRuntimeInstanceID == committed.Operation.Coordinate.RuntimeInstanceID ||
		publication.OperationRuntimeInstanceID != publication.ActivationRuntimeInstanceID ||
		publication.ActivationStatus != string(channelonboarding.ActivationCurrent) {
		t.Fatalf("%s E2E-14 exact recovery publication = committed %#v publication %#v", backend, committed, publication)
	}
	if count := channelOnboardingCredentialCount(t, harness.credentialPath); count <= predecessorCredentialCount {
		t.Fatalf("%s E2E-14 recovery cleaned predecessor before promotion: count=%d predecessor=%d", backend, count, predecessorCredentialCount)
	}
	if registrations, deliveries := harness.provider.Counts(); registrations != 3 || deliveries != 1 {
		t.Fatalf("%s E2E-14 recovery effects before promotion=%d/%d, want exact startup renewal count 3 and predecessor confirmation count 1", backend, registrations, deliveries)
	}
	publicationBarrier.Release()
	harness.process.waitForReadyLine()
	harness.opts.TestChannelOnboardingBarrier = nil
	return publication
}

func assertChannelOnboardingServeNotReady(t *testing.T, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := (&http.Client{Timeout: time.Second}).Get(endpoint + "/readyz")
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("restart readiness status=%d, want 503 before registration promotion", response.StatusCode)
		}
		return
	}
	t.Fatalf("restart readiness endpoint %s did not become observable", endpoint)
}

func runChannelOnboardingDestructiveCrashBoundary(t *testing.T, backend servedparity.Backend, harness *channelOnboardingE2EHarness, barrier, uncheckpointedBarrier *channelOnboardingTestBarrier) {
	t.Helper()
	ready := runChannelOnboardingCLIJourney(t, harness.opts.ConfigPath, harness.endpoint, harness.provider, "connect", "destructive-token", 7217, "private", 0)
	if ready.Activation == nil || ready.Readiness == nil || !ready.Readiness.Ready {
		t.Fatalf("%s E2E-17 prerequisite channel = %#v", backend, ready)
	}
	checkpointed := getChannelOnboardingRPC(t, harness, ready.Operation.OperationID)
	checkpointedCredentialCount := channelOnboardingCredentialCount(t, harness.credentialPath)
	callbackURL, signingSecret, _ := harness.provider.Registration()
	uncheckpointedBarrier.Arm()
	stagedCommand := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint,
		[]string{"channel", "reconnect", "telegram", "--yes", "--credential-stdin"}, "destructive-uncheckpointed-token\n")
	stagedOperationID := uncheckpointedBarrier.Wait(t)
	staged := getChannelOnboardingRPC(t, harness, stagedOperationID)
	if staged.Operation.OperationID == checkpointed.Operation.OperationID || staged.Operation.Phase != channelonboarding.PhasePreparing || len(staged.Operation.CredentialAdmissions) != 0 {
		t.Fatalf("%s E2E-17 uncheckpointed operation = %#v, checkpointed=%s", backend, staged.Operation, checkpointed.Operation.OperationID)
	}
	if count := channelOnboardingCredentialCount(t, harness.credentialPath); count <= checkpointedCredentialCount {
		t.Fatalf("%s E2E-17 uncheckpointed occurrence count=%d, want more than checkpointed %d", backend, count, checkpointedCredentialCount)
	}
	command := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, []string{"channel", "unbind", ready.Identity.Interface.Selector}, "")
	teardownID := barrier.Wait(t)
	if teardownID == "" {
		t.Fatal("E2E-17 teardown barrier lacks responsibility ID")
	}
	assertChannelOnboardingCredentialStoreNotEmpty(t, harness.credentialPath, string(backend)+" E2E-17 before cleanup")
	crashChannelOnboardingServeAtBarriers(t, harness, []*channelOnboardingTestBarrier{barrier, uncheckpointedBarrier}, []channelOnboardingCLICommand{command, stagedCommand})
	harness.opts.TestChannelOnboardingBarrier = nil
	harness.start(t)
	unbound := readChannelOnboardingRow(t, harness.opts.ConfigPath, harness.endpoint, "unbound")
	if unbound.Activation != nil || unbound.Readiness != nil || unbound.Identity.BindingRevision <= ready.Identity.BindingRevision {
		t.Fatalf("%s E2E-17 recovered teardown = %#v", backend, unbound)
	}
	assertChannelOnboardingCredentialStoreEmpty(t, harness.credentialPath, string(backend)+" E2E-17 recovered cleanup")
	retiredStaged := getChannelOnboardingRPC(t, harness, stagedOperationID)
	if retiredStaged.Operation.Phase != channelonboarding.PhaseRetired || retiredStaged.Operation.FailureCode != "authority_retired" {
		t.Fatalf("%s E2E-17 uncheckpointed operation resurrected: %#v", backend, retiredStaged.Operation)
	}
	retiredAfterRestart := submitChannelOnboardingClaim(t, callbackURL, signingSecret, "SWARM-AAAAAAAAAAAAAAAA", 7219, "retired_after_restart")
	if retiredAfterRestart.StatusCode != http.StatusNotFound {
		t.Fatalf("%s E2E-17 registration authority resurrected after restart: %#v", backend, retiredAfterRestart)
	}
	if registrations, deliveries := harness.provider.Counts(); registrations != 1 || deliveries != 1 {
		t.Fatalf("%s E2E-17 replayed provider effects = %d/%d, want 1/1", backend, registrations, deliveries)
	}
	harness.stop(t)
}

type channelOnboardingTestBarrier struct {
	want     channelonboarding.TestLifecycleBoundary
	arrived  chan string
	release  chan struct{}
	mu       sync.RWMutex
	armed    bool
	fail     bool
	once     sync.Once
	released sync.Once
}

func newChannelOnboardingTestBarrier(want channelonboarding.TestLifecycleBoundary) *channelOnboardingTestBarrier {
	barrier := &channelOnboardingTestBarrier{want: want, arrived: make(chan string, 1), release: make(chan struct{}), armed: true, fail: true}
	return barrier
}

func (b *channelOnboardingTestBarrier) Release() {
	if b == nil {
		return
	}
	b.released.Do(func() { close(b.release) })
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

func (b *channelOnboardingTestBarrier) ContinueAfterRelease() {
	b.mu.Lock()
	b.fail = false
	b.mu.Unlock()
}

func (b *channelOnboardingTestBarrier) Reach(boundary channelonboarding.TestLifecycleBoundary, responsibilityID string) error {
	if b == nil || boundary != b.want {
		return nil
	}
	b.mu.RLock()
	armed, fail := b.armed, b.fail
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
	if blocked && fail {
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
	crashChannelOnboardingServeAtBarriers(t, harness, []*channelOnboardingTestBarrier{barrier}, []channelOnboardingCLICommand{command})
}

func crashChannelOnboardingServeAtBarriers(t *testing.T, harness *channelOnboardingE2EHarness, barriers []*channelOnboardingTestBarrier, commands []channelOnboardingCLICommand) {
	t.Helper()
	harness.process.cancel()
	for _, barrier := range barriers {
		barrier.Release()
	}
	for _, command := range commands {
		select {
		case code := <-command.done:
			if code == 0 {
				t.Fatalf("channel command succeeded across injected process loss\nstdout:\n%s\nstderr:\n%s", command.stdout.String(), command.stderr.String())
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("channel command did not settle after injected process loss\nstdout:\n%s\nstderr:\n%s", command.stdout.String(), command.stderr.String())
		}
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

package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil"
)

var channelOnboardingExactChoicePattern = regexp.MustCompile(`--bundle (\S+) --interface (\S+) --target (\S+)`)

type channelOnboardingExactChoice struct {
	bundle       string
	interfaceRef string
	target       string
}

func TestChannelOnboardingE2E07ClaimTimeoutResume(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			harness := newChannelOnboardingE2EHarness(t, backend, true)
			harness.start(t)

			input := installChannelOnboardingCLIInput(t, "timeout-token\n")
			defer input()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
			code := executeCLI(ctx, []string{
				"--config", harness.opts.ConfigPath, "channel", "connect", "telegram", "--yes", "--api-server", harness.endpoint,
			}, stdout, stderr, nil)
			surface := stdout.String() + "\n" + stderr.String()
			operationID := channelOnboardingOperationIDPattern.FindString(surface)
			resumeCommand := "swarm channel resume " + operationID
			if code == 0 || operationID == "" || !strings.Contains(surface, resumeCommand) {
				t.Fatalf("%s E2E-07 timeout code=%d lacks exact resume\nstdout:\n%s\nstderr:\n%s", backend, code, stdout.String(), stderr.String())
			}
			row := requireChannelOnboardingOperationRow(t, harness, operationID)
			if row.Operation.Phase != string(channelonboarding.PhaseAwaitingExternalIdentity) || row.Recovery == nil || len(row.Recovery.Commands) != 1 || row.Recovery.Commands[0] != resumeCommand {
				t.Fatalf("%s E2E-07 timeout readback = %#v", backend, row)
			}

			command := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, []string{"channel", "resume", operationID, "--yes"}, "")
			challenge := waitChannelOnboardingChallenge(t, command.stdout, command.stderr, command.done)
			callbackURL, signingSecret := waitChannelOnboardingRegistration(t, harness.provider, command.stdout, command.stderr, command.done)
			admission := submitChannelOnboardingClaim(t, callbackURL, signingSecret, challenge, 7107, "timeout_resume")
			requireChannelClaimDisposition(t, "E2E-07 resumed claim", admission, "consumed_by_binding")
			requireChannelOnboardingCommandSuccess(t, command)
			ready := requireChannelOnboardingOperationRow(t, harness, operationID)
			if ready.Operation.Phase != string(channelonboarding.PhaseSucceeded) || ready.Readiness == nil || !ready.Readiness.Ready {
				t.Fatalf("%s E2E-07 resumed row = %#v", backend, ready)
			}
			harness.stop(t)
		})
	}
}

func TestChannelOnboardingE2E08OperatorRejection(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			harness := newChannelOnboardingE2EHarness(t, backend, true)
			harness.start(t)
			begun := startChannelOnboardingRPC(t, harness, channelonboarding.VerbConnect, "rejection-token", nil)
			if begun.IdentityOperation == nil {
				t.Fatalf("%s E2E-08 start lacks identity operation: %#v", backend, begun)
			}
			callbackURL, signingSecret, _ := harness.provider.Registration()
			admission := submitChannelOnboardingClaim(t, callbackURL, signingSecret, begun.IdentityOperation.Challenge, 7108, "rejected_operator")
			requireChannelClaimDisposition(t, "E2E-08 claim", admission, "consumed_by_binding")
			claimed := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
			if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
				t.Fatalf("%s E2E-08 claimed operation = %#v", backend, claimed)
			}
			var rejected map[string]any
			requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.confirm", map[string]any{
				"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": false,
			}, &rejected)
			settled := retryChannelOnboardingRPC(t, harness, begun.Operation.OperationID, "")
			if settled.Operation.Phase != channelonboarding.PhaseFailed || settled.Operation.FailureCode != "identity_rejected" || settled.Readiness == nil || settled.Readiness.Ready {
				t.Fatalf("%s E2E-08 settled result = %#v", backend, settled)
			}
			assertNoChannelOnboardingActivation(t, harness, begun.Operation.OperationID)
			assertChannelOnboardingCredentialStoreEmpty(t, harness.credentialPath, string(backend)+" E2E-08 rejection")
			if registrations, deliveries := harness.provider.Counts(); registrations != 1 || deliveries != 0 {
				t.Fatalf("%s E2E-08 provider effects = %d/%d, want 1/0", backend, registrations, deliveries)
			}
			retired := submitChannelOnboardingClaim(t, callbackURL, signingSecret, begun.IdentityOperation.Challenge, 7109, "rejected_after_settlement")
			if retired.StatusCode != http.StatusNotFound {
				t.Fatalf("%s E2E-08 rejected callback remained admitted: %#v", backend, retired)
			}
			assertChannelOnboardingNoRecoveryGuidance(t, harness, begun.Operation.OperationID)
			reacquired := startChannelOnboardingRPC(t, harness, channelonboarding.VerbConnect, "reacquired-token", nil)
			if reacquired.Operation.OperationID == begun.Operation.OperationID || reacquired.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity {
				t.Fatalf("%s E2E-08 slot reacquisition = %#v", backend, reacquired.Operation)
			}
			harness.stop(t)
		})
	}
}

func TestChannelOnboardingE2E09ExpiryAcrossRestart(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			now := time.Date(2026, 8, 28, 8, 0, 0, 0, time.UTC)
			harness := newChannelOnboardingE2EHarness(t, backend, true)
			harness.opts.TestChannelOnboardingNow = func() time.Time { return now }
			harness.start(t)
			begun := startChannelOnboardingRPC(t, harness, channelonboarding.VerbConnect, "expiry-token", nil)
			if begun.IdentityOperation == nil {
				t.Fatalf("%s E2E-09 start lacks identity operation: %#v", backend, begun)
			}
			callbackURL, signingSecret, _ := harness.provider.Registration()
			challenge := begun.IdentityOperation.Challenge
			harness.stop(t)

			now = now.Add(11 * time.Minute)
			harness.start(t)
			expired := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
			if expired.Operation.Phase != channelonboarding.PhaseFailed || expired.Operation.FailureCode != "identity_expired" || expired.Readiness == nil || expired.Readiness.Ready {
				t.Fatalf("%s E2E-09 recovered expiry = %#v", backend, expired)
			}
			assertNoChannelOnboardingActivation(t, harness, begun.Operation.OperationID)
			assertChannelOnboardingCredentialStoreEmpty(t, harness.credentialPath, string(backend)+" E2E-09 expiry")
			late := submitChannelOnboardingClaim(t, callbackURL, signingSecret, challenge, 7109, "late_operator")
			if late.StatusCode != http.StatusNotFound || len(late.EventIDs) != 0 || len(late.EventNames) != 0 {
				t.Fatalf("%s E2E-09 late retired callback = %#v, want unavailable route and zero business events", backend, late)
			}
			afterLate := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
			if afterLate.Operation.Revision != expired.Operation.Revision || afterLate.Operation.Phase != channelonboarding.PhaseFailed {
				t.Fatalf("%s E2E-09 late claim changed failed history: before=%#v after=%#v", backend, expired.Operation, afterLate.Operation)
			}
			reacquired := startChannelOnboardingRPC(t, harness, channelonboarding.VerbConnect, "post-expiry-token", nil)
			if reacquired.Operation.OperationID == begun.Operation.OperationID || reacquired.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity {
				t.Fatalf("%s E2E-09 slot reacquisition = %#v", backend, reacquired.Operation)
			}
			harness.stop(t)
		})
	}
}

func TestChannelOnboardingE2E12MultipleExactContexts(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			harness := newChannelOnboardingE2EHarness(t, backend, true)
			addSecondChannelOnboardingTarget(t, harness.opts.ContractsPath)
			harness.start(t)

			choices := requireAmbiguousChannelOnboardingChoices(t, harness, 2)
			first, second := choices[0], choices[1]
			firstCommand := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, exactChannelOnboardingArgs("connect", first), "multi-context-token\n")
			challenge := waitChannelOnboardingChallenge(t, firstCommand.stdout, firstCommand.stderr, firstCommand.done)
			callbackURL, signingSecret := waitChannelOnboardingRegistration(t, harness.provider, firstCommand.stdout, firstCommand.stderr, firstCommand.done)
			requireChannelClaimDisposition(t, "E2E-12 first exact claim", submitChannelOnboardingClaim(t, callbackURL, signingSecret, challenge, 7212, "multi_context"), "consumed_by_binding")
			requireChannelOnboardingCommandSuccess(t, firstCommand)
			_ = waitChannelOnboardingDelivery(t, harness.provider, 0)

			firstReady := requireChannelOnboardingTargetRow(t, harness, first.target)
			if firstReady.Activation == nil || firstReady.Readiness == nil || !firstReady.Readiness.Ready {
				t.Fatalf("%s E2E-12 first exact context = %#v", backend, firstReady)
			}

			blockedOut, blockedErr := &lockedBuffer{}, &lockedBuffer{}
			blockedCode := executeCLI(context.Background(), append([]string{"--config", harness.opts.ConfigPath}, append(exactChannelOnboardingArgs("reconnect", second), "--api-server", harness.endpoint)...), blockedOut, blockedErr, nil)
			blockedSurface := blockedOut.String() + "\n" + blockedErr.String()
			blockedOperationID := channelOnboardingOperationIDPattern.FindString(blockedSurface)
			if blockedCode == 0 || blockedOperationID == "" || !strings.Contains(blockedSurface, "CHANNEL_CREDENTIAL_REQUIRED") {
				t.Fatalf("%s E2E-12 second exact context code=%d\nstdout:\n%s\nstderr:\n%s", backend, blockedCode, blockedOut.String(), blockedErr.String())
			}

			blocked := requireChannelOnboardingTargetRow(t, harness, second.target)
			if blocked.Operation == nil || blocked.Operation.OperationID != blockedOperationID || blocked.Operation.Phase != string(channelonboarding.PhasePreparing) || blocked.Activation != nil {
				t.Fatalf("%s E2E-12 blocked sibling context = %#v", backend, blocked)
			}
			assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-12 sibling identity", firstReady, blocked)

			firstReconnect := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, exactChannelOnboardingArgs("reconnect", first), "")
			requireChannelOnboardingCommandSuccess(t, firstReconnect)
			_ = waitChannelOnboardingDelivery(t, harness.provider, 1)

			advanced := requireChannelOnboardingTargetRow(t, harness, first.target)
			stillBlocked := requireChannelOnboardingTargetRow(t, harness, second.target)
			assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-12 exact reconnect", firstReady, advanced)
			if advanced.Activation == nil || firstReady.Activation == nil || advanced.Activation.Revision <= firstReady.Activation.Revision ||
				advanced.Readiness == nil || !advanced.Readiness.Ready || advanced.Readiness.ActivationGeneration == firstReady.Readiness.ActivationGeneration {
				t.Fatalf("%s E2E-12 exact reconnect did not advance selected context: before=%#v after=%#v", backend, firstReady, advanced)
			}
			if stillBlocked.Operation == nil || stillBlocked.Operation.OperationID != blocked.Operation.OperationID || stillBlocked.Operation.Phase != blocked.Operation.Phase || stillBlocked.Activation != nil {
				t.Fatalf("%s E2E-12 exact reconnect mutated sibling context: before=%#v after=%#v", backend, blocked, stillBlocked)
			}
			if registrations, deliveries := harness.provider.Counts(); registrations != 2 || deliveries != 2 {
				t.Fatalf("%s E2E-12 provider effects = %d/%d, want one exact registration and confirmation per completed operation", backend, registrations, deliveries)
			}
			requireAmbiguousChannelOnboardingChoices(t, harness, 2)
			harness.stop(t)
		})
	}
}

func TestChannelOnboardingE2E18IngressUnavailable(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			harness := newChannelOnboardingE2EHarness(t, backend, false)
			harness.start(t)
			const secret = "no-ingress-token-e2e18-private"
			startEnvelope := requestServedJSONRPC(t, harness.rpcEndpoint(), "channel.onboarding_start", map[string]any{
				"provider": "telegram", "verb": string(channelonboarding.VerbConnect), "provider_credential": secret, "save_proof": true,
			})
			if startEnvelope.Error != nil {
				t.Fatalf("%s E2E-18 start error = %#v", backend, startEnvelope.Error)
			}
			var result channelonboarding.Result
			if err := json.Unmarshal(startEnvelope.Result, &result); err != nil {
				t.Fatalf("%s E2E-18 decode start result: %v\n%s", backend, err, startEnvelope.Result)
			}
			if result.Operation.Phase != channelonboarding.PhaseFailed || result.Operation.FailureCode != "public_ingress_unavailable" || result.Readiness == nil || result.Readiness.Ready {
				t.Fatalf("%s E2E-18 result = %#v", backend, result)
			}
			assertNoChannelOnboardingActivation(t, harness, result.Operation.OperationID)
			if registrations, deliveries := harness.provider.Counts(); registrations != 0 || deliveries != 0 {
				t.Fatalf("%s E2E-18 provider effects = %d/%d, want 0/0", backend, registrations, deliveries)
			}
			assertChannelOnboardingCredentialStoreEmpty(t, harness.credentialPath, string(backend)+" E2E-18 no ingress")
			getEnvelope := requestServedJSONRPC(t, harness.rpcEndpoint(), "channel.onboarding_get", map[string]any{"operation_id": result.Operation.OperationID})
			jsonList, humanList := channelOnboardingListSurfaces(t, harness)
			assertChannelOnboardingSecretAbsent(t, secret, map[string]string{
				"start RPC result/error": mustMarshalChannelOnboardingSurface(t, startEnvelope),
				"get RPC result/error":   mustMarshalChannelOnboardingSurface(t, getEnvelope),
				"JSON list":              jsonList,
				"human list":             humanList,
				"serve output/logs":      harness.process.outputString(),
				"selected-store row":     channelOnboardingPersistedOperationSurface(t, harness, result.Operation.OperationID),
			})
			harness.stop(t)
		})
	}
}

func TestChannelOnboardingE2E19ChallengeAdmissionBoundaries(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			harness := newChannelOnboardingE2EHarness(t, backend, true)
			harness.start(t)
			begun := startChannelOnboardingRPC(t, harness, channelonboarding.VerbConnect, "challenge-token", nil)
			callbackURL, signingSecret, _ := harness.provider.Registration()
			beforeWrong := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
			wrong := submitChannelOnboardingClaim(t, callbackURL, signingSecret, "SWARM-AAAAAAAAAAAAAAAA", 7119, "wrong_operator")
			requireChannelClaimDisposition(t, "E2E-19 wrong claim", wrong, "rejected_binding_claim")
			afterWrong := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
			assertChannelOnboardingClaimDidNotMutate(t, string(backend)+" E2E-19 wrong", beforeWrong, afterWrong)

			correct := submitChannelOnboardingClaimAs(t, callbackURL, signingSecret, begun.IdentityOperation.Challenge, 7120, 8120, 9120, "current_operator")
			requireChannelClaimDisposition(t, "E2E-19 correct claim", correct, "consumed_by_binding")
			claimed := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
			duplicate := submitChannelOnboardingClaimAs(t, callbackURL, signingSecret, begun.IdentityOperation.Challenge, 7121, 8120, 9120, "current_operator")
			requireChannelClaimDisposition(t, "E2E-19 duplicate claim", duplicate, "consumed_by_binding")
			afterDuplicate := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
			assertChannelOnboardingClaimDidNotMutate(t, string(backend)+" E2E-19 duplicate", claimed, afterDuplicate)

			var confirmed map[string]any
			requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.confirm", map[string]any{
				"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
			}, &confirmed)
			succeeded := retryChannelOnboardingRPC(t, harness, begun.Operation.OperationID, "")
			if succeeded.Operation.Phase != channelonboarding.PhaseSucceeded || succeeded.Readiness == nil || !succeeded.Readiness.Ready {
				t.Fatalf("%s E2E-19 succeeded result = %#v", backend, succeeded)
			}
			late := submitChannelOnboardingClaimAs(t, callbackURL, signingSecret, begun.IdentityOperation.Challenge, 7122, 8120, 9120, "current_operator")
			requireChannelClaimDisposition(t, "E2E-19 late claim", late, "rejected_binding_claim")
			afterLate := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
			if afterLate.Operation.Revision != succeeded.Operation.Revision || afterLate.Operation.Phase != channelonboarding.PhaseSucceeded || afterLate.Readiness == nil || !afterLate.Readiness.Ready {
				t.Fatalf("%s E2E-19 late claim changed current channel: before=%#v after=%#v", backend, succeeded, afterLate)
			}
			if registrations, deliveries := harness.provider.Counts(); registrations != 1 || deliveries != 1 {
				t.Fatalf("%s E2E-19 provider effects = %d/%d, want 1/1", backend, registrations, deliveries)
			}
			harness.stop(t)
		})
	}
}

func TestChannelOnboardingE2E20FailedReplacementPreservesPredecessor(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			harness := newChannelOnboardingE2EHarness(t, backend, true)
			harness.start(t)
			predecessor := runChannelOnboardingCLIJourney(t, harness.opts.ConfigPath, harness.endpoint, harness.provider, "connect", "predecessor-token", 7220, "private", 0)
			if predecessor.Operation == nil || predecessor.Activation == nil || predecessor.Readiness == nil || !predecessor.Readiness.Ready {
				t.Fatalf("%s E2E-20 predecessor = %#v", backend, predecessor)
			}

			replacement := startChannelOnboardingRPC(t, harness, channelonboarding.VerbRebind, "", nil)
			if replacement.IdentityOperation == nil || replacement.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity {
				t.Fatalf("%s E2E-20 replacement start = %#v", backend, replacement)
			}
			callbackURL, signingSecret, _ := harness.provider.Registration()
			requireChannelClaimDisposition(t, "E2E-20 replacement claim", submitChannelOnboardingClaim(t, callbackURL, signingSecret, replacement.IdentityOperation.Challenge, 8220, "rejected_replacement"), "consumed_by_binding")
			claimed := getChannelOnboardingRPC(t, harness, replacement.Operation.OperationID)
			if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
				t.Fatalf("%s E2E-20 replacement claim = %#v", backend, claimed)
			}
			var rejected map[string]any
			requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.confirm", map[string]any{
				"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": false,
			}, &rejected)
			failed := retryChannelOnboardingRPC(t, harness, replacement.Operation.OperationID, "")
			if failed.Operation.Phase != channelonboarding.PhaseFailed || failed.Operation.FailureCode != "identity_rejected" {
				t.Fatalf("%s E2E-20 replacement terminal result = %#v", backend, failed)
			}

			current := requireChannelOnboardingOperationRow(t, harness, predecessor.Operation.OperationID)
			assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-20 predecessor", predecessor, current)
			if current.Activation == nil || current.Readiness == nil || !current.Readiness.Ready || current.Operation == nil || current.Operation.OperationID != predecessor.Operation.OperationID ||
				current.Activation.Revision != predecessor.Activation.Revision || current.Readiness.ActivationGeneration != predecessor.Readiness.ActivationGeneration {
				t.Fatalf("%s E2E-20 failed replacement displaced predecessor: before=%#v after=%#v", backend, predecessor, current)
			}
			for _, row := range readChannelOnboardingRows(t, harness.opts.ConfigPath, harness.endpoint) {
				if row.Operation != nil && row.Operation.OperationID == replacement.Operation.OperationID {
					t.Fatalf("%s E2E-20 terminal replacement replaced current readback: %#v", backend, row)
				}
			}
			if registrations, deliveries := harness.provider.Counts(); registrations != 1 || deliveries != 1 {
				t.Fatalf("%s E2E-20 provider effects = %d/%d, want retained predecessor registration and confirmation only", backend, registrations, deliveries)
			}
			harness.stop(t)
		})
	}
}

func requireAmbiguousChannelOnboardingChoices(t *testing.T, harness *channelOnboardingE2EHarness, want int) []channelOnboardingExactChoice {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	code := executeCLI(context.Background(), []string{
		"--config", harness.opts.ConfigPath, "channel", "reconnect", "telegram", "--yes", "--api-server", harness.endpoint,
	}, stdout, stderr, nil)
	surface := stdout.String() + "\n" + stderr.String()
	matches := channelOnboardingExactChoicePattern.FindAllStringSubmatch(surface, -1)
	if code == 0 || !strings.Contains(surface, "ambiguous") || len(matches) != want {
		t.Fatalf("ambiguous channel shorthand code=%d choices=%d, want %d\nstdout:\n%s\nstderr:\n%s", code, len(matches), want, stdout.String(), stderr.String())
	}
	choices := make([]channelOnboardingExactChoice, 0, len(matches))
	for _, match := range matches {
		choices = append(choices, channelOnboardingExactChoice{bundle: match[1], interfaceRef: match[2], target: strings.TrimRight(match[3], ";,)")})
	}
	sort.Slice(choices, func(i, j int) bool { return choices[i].target < choices[j].target })
	return choices
}

func exactChannelOnboardingArgs(verb string, choice channelOnboardingExactChoice) []string {
	return []string{
		"channel", verb, "telegram", "--yes",
		"--bundle", choice.bundle, "--interface", choice.interfaceRef, "--target", choice.target,
	}
}

func requireChannelOnboardingTargetRow(t *testing.T, harness *channelOnboardingE2EHarness, target string) channelOnboardingJourneyReadback {
	t.Helper()
	for _, row := range readChannelOnboardingRows(t, harness.opts.ConfigPath, harness.endpoint) {
		if row.Operation != nil && row.Operation.TargetSelector == target {
			return row
		}
	}
	t.Fatalf("channel list has no exact target %s", target)
	return channelOnboardingJourneyReadback{}
}

func addSecondChannelOnboardingTarget(t *testing.T, contractsRoot string) {
	t.Helper()
	sourceDir := filepath.Join(contractsRoot, "flows", "telegram-ingress")
	targetDir := filepath.Join(contractsRoot, "flows", "telegram-ingress-alt")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("create second channel onboarding target: %v", err)
	}
	for _, name := range []string{"entities.yaml", "schema.yaml"} {
		contents, err := os.ReadFile(filepath.Join(sourceDir, name))
		if err != nil {
			t.Fatalf("read second channel onboarding target source %s: %v", name, err)
		}
		if name == "schema.yaml" {
			contents = []byte(strings.Replace(string(contents), "name: telegram-ingress", "name: telegram-ingress-alt", 1))
		}
		if err := os.WriteFile(filepath.Join(targetDir, name), contents, 0o644); err != nil {
			t.Fatalf("write second channel onboarding target %s: %v", name, err)
		}
	}
	rootPackage := `name: telegram-agent
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - id: bot
    path: bot
flows:
  - id: telegram-ingress
    flow: telegram-ingress
    mode: singleton
    activation: standing
    ingress:
      alias: chat
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
  - id: telegram-ingress-alt
    flow: telegram-ingress-alt
    mode: singleton
    activation: standing
    ingress:
      alias: support
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
`
	if err := os.WriteFile(filepath.Join(contractsRoot, "package.yaml"), []byte(rootPackage), 0o644); err != nil {
		t.Fatalf("write multi-context channel onboarding package: %v", err)
	}
}

type channelOnboardingE2EHarness struct {
	opts           cliapp.ServeOptions
	provider       *channelOnboardingTelegramProvider
	telegram       *httptest.Server
	credentialPath string
	backend        servedparity.Backend
	storeDSN       string
	endpoint       string
	process        *serveRuntimeTestProcess
}

func newChannelOnboardingE2EHarness(t *testing.T, backend servedparity.Backend, publicIngress bool) *channelOnboardingE2EHarness {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	provider := &channelOnboardingTelegramProvider{}
	telegram := httptest.NewServer(provider)
	t.Cleanup(telegram.Close)
	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	disableChannelOnboardingBusinessConsumers(t, contractsRoot)
	opts := cliapp.ServeOptions{
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, AbandonActiveRuns: true, Verbose: true,
		WorkspaceBackend: "host", WorkspaceBackendSet: true, TestLLMRuntime: telegramPhraseBotLLMRuntime{},
	}
	if publicIngress {
		publicListen := reserveChannelOnboardingListenAddress(t)
		redirectExternalHosts(t, map[string]string{"hooks.channel-onboarding.test": "http://" + publicListen})
		opts.PublicWebhookBaseURL = "https://hooks.channel-onboarding.test"
		opts.PublicWebhookListen = publicListen
	}
	storeDSN := ""
	switch backend {
	case servedparity.BackendDefaultSQLite:
		sqlitePath := filepath.Join(t.TempDir(), "channel-onboarding-e2e.sqlite")
		storeDSN = sqlitePath
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, channelOnboardingHostWorkspaceFields())
		opts.StoreMode = "sqlite"
	case servedparity.BackendExplicitPostgres:
		dsn, _, _ := testutil.StartPostgres(t)
		storeDSN = dsn
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
		opts.StoreMode = "postgres"
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	opts.StoreModeSet = true
	enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
	return &channelOnboardingE2EHarness{opts: opts, provider: provider, telegram: telegram, credentialPath: credentialPath, backend: backend, storeDSN: storeDSN}
}

func (h *channelOnboardingE2EHarness) start(t *testing.T) {
	t.Helper()
	h.process = startServeRuntimeTestProcess(t, h.opts)
	h.process.waitForReadyLine()
	h.endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, h.process.outputString())
}

func (h *channelOnboardingE2EHarness) stop(t *testing.T) {
	t.Helper()
	if h.process == nil {
		return
	}
	if code := h.process.stop(); code != 0 {
		t.Fatalf("channel onboarding E2E serve exit = %d\n%s", code, h.process.outputString())
	}
	h.process = nil
}

func (h *channelOnboardingE2EHarness) rpcEndpoint() string {
	return strings.TrimRight(h.endpoint, "/") + "/v1/rpc"
}

func startChannelOnboardingRPC(t *testing.T, harness *channelOnboardingE2EHarness, verb channelonboarding.Verb, credential string, selectors map[string]string) channelonboarding.Result {
	t.Helper()
	params := map[string]any{"provider": "telegram", "verb": string(verb), "provider_credential": credential, "save_proof": true}
	for key, value := range selectors {
		params[key] = value
	}
	var result channelonboarding.Result
	requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.onboarding_start", params, &result)
	return result
}

func getChannelOnboardingRPC(t *testing.T, harness *channelOnboardingE2EHarness, operationID string) channelonboarding.Result {
	t.Helper()
	var result channelonboarding.Result
	requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.onboarding_get", map[string]any{"operation_id": operationID}, &result)
	return result
}

func retryChannelOnboardingRPC(t *testing.T, harness *channelOnboardingE2EHarness, operationID, credential string) channelonboarding.Result {
	t.Helper()
	params := map[string]any{"operation_id": operationID}
	if credential != "" {
		params["provider_credential"] = credential
	}
	var result channelonboarding.Result
	requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.onboarding_retry", params, &result)
	return result
}

func assertChannelOnboardingNoRecoveryGuidance(t *testing.T, harness *channelOnboardingE2EHarness, operationID string) {
	t.Helper()
	jsonList, humanList := channelOnboardingListSurfaces(t, harness)
	resume := "swarm channel resume " + operationID
	for label, surface := range map[string]string{"JSON list": jsonList, "human list": humanList} {
		if strings.Contains(surface, operationID) || strings.Contains(surface, resume) {
			t.Fatalf("%s retained recovery guidance for rejected operation %s:\n%s", label, operationID, surface)
		}
	}
}

func channelOnboardingListSurfaces(t *testing.T, harness *channelOnboardingE2EHarness) (string, string) {
	t.Helper()
	run := func(jsonOutput bool) string {
		stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
		args := []string{"--config", harness.opts.ConfigPath, "channel", "list", "--api-server", harness.endpoint}
		if jsonOutput {
			args = append(args, "--json")
		}
		if code := executeCLI(context.Background(), args, stdout, stderr, nil); code != 0 {
			t.Fatalf("channel list exited %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
		}
		return stdout.String() + "\n" + stderr.String()
	}
	return run(true), run(false)
}

func assertChannelOnboardingSecretAbsent(t *testing.T, secret string, surfaces map[string]string) {
	t.Helper()
	digest := runtimeeffects.Fingerprint([]byte(secret))
	for label, surface := range surfaces {
		if strings.Contains(surface, secret) || strings.Contains(strings.ToLower(surface), strings.ToLower(digest)) {
			t.Fatalf("%s leaked the provider credential or its reusable digest", label)
		}
	}
}

func mustMarshalChannelOnboardingSurface(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal channel onboarding surface: %v", err)
	}
	return string(raw)
}

func channelOnboardingPersistedOperationSurface(t *testing.T, harness *channelOnboardingE2EHarness, operationID string) string {
	t.Helper()
	driver, placeholder := "sqlite", "?"
	if harness.backend == servedparity.BackendExplicitPostgres {
		driver, placeholder = "postgres", "$1"
	}
	db, err := sql.Open(driver, harness.storeDSN)
	if err != nil {
		t.Fatalf("open %s channel onboarding store: %v", harness.backend, err)
	}
	defer db.Close()
	rows, err := db.QueryContext(context.Background(), "SELECT * FROM channel_onboarding_operations WHERE operation_id="+placeholder, operationID)
	if err != nil {
		t.Fatalf("query %s channel onboarding operation: %v", harness.backend, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("read %s channel onboarding columns: %v", harness.backend, err)
	}
	if !rows.Next() {
		t.Fatalf("%s selected store has no channel onboarding operation %s", harness.backend, operationID)
	}
	values := make([]any, len(columns))
	destinations := make([]any, len(columns))
	for index := range values {
		destinations[index] = &values[index]
	}
	if err := rows.Scan(destinations...); err != nil {
		t.Fatalf("scan %s channel onboarding operation: %v", harness.backend, err)
	}
	record := make(map[string]string, len(columns))
	for index, column := range columns {
		switch value := values[index].(type) {
		case []byte:
			record[column] = string(value)
		default:
			record[column] = fmt.Sprint(value)
		}
	}
	return mustMarshalChannelOnboardingSurface(t, record)
}

type channelOnboardingPersistedHandoff struct {
	OperationPhase              string
	OperationActivationRevision int64
	OperationRuntimeInstanceID  string
	ActivationRevision          int64
	ActivationRuntimeInstanceID string
	ActivationStatus            string
}

func channelOnboardingPersistedHandoffAtPublication(t *testing.T, harness *channelOnboardingE2EHarness, operationID string) channelOnboardingPersistedHandoff {
	t.Helper()
	driver, placeholder := "sqlite", "?"
	if harness.backend == servedparity.BackendExplicitPostgres {
		driver, placeholder = "postgres", "$1"
	}
	db, err := sql.Open(driver, harness.storeDSN)
	if err != nil {
		t.Fatalf("open %s channel onboarding store: %v", harness.backend, err)
	}
	defer db.Close()
	query := `SELECT operation.phase,operation.activation_revision,operation.runtime_instance_id,
		activation.activation_revision,activation.runtime_instance_id,activation.status
		FROM channel_onboarding_operations operation
		JOIN connected_channel_activations activation ON activation.operation_id=operation.operation_id
		WHERE operation.operation_id=` + placeholder + ` ORDER BY activation.activation_revision DESC LIMIT 1`
	var out channelOnboardingPersistedHandoff
	if err := db.QueryRowContext(context.Background(), query, operationID).Scan(
		&out.OperationPhase, &out.OperationActivationRevision, &out.OperationRuntimeInstanceID,
		&out.ActivationRevision, &out.ActivationRuntimeInstanceID, &out.ActivationStatus,
	); err != nil {
		t.Fatalf("query %s channel onboarding publication handoff: %v", harness.backend, err)
	}
	return out
}

type channelOnboardingClaimAdmission struct {
	Disposition string   `json:"operator_channel_claim_disposition"`
	EventIDs    []string `json:"event_ids"`
	EventNames  []string `json:"event_names"`
	StatusCode  int      `json:"-"`
	Body        string   `json:"-"`
}

func submitChannelOnboardingClaim(t *testing.T, callbackURL, signingSecret, challenge string, updateID int64, username string) channelOnboardingClaimAdmission {
	t.Helper()
	return submitChannelOnboardingClaimAs(t, callbackURL, signingSecret, challenge, updateID, updateID+1000, updateID+2000, username)
}

func submitChannelOnboardingClaimAs(t *testing.T, callbackURL, signingSecret, challenge string, updateID, claimantID, chatID int64, username string) channelOnboardingClaimAdmission {
	t.Helper()
	return submitChannelOnboardingClaimWithChatType(t, callbackURL, signingSecret, challenge, updateID, claimantID, chatID, "private", username)
}

func submitChannelOnboardingClaimWithChatType(t *testing.T, callbackURL, signingSecret, challenge string, updateID, claimantID, chatID int64, chatType, username string) channelOnboardingClaimAdmission {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"update_id": updateID,
		"message": map[string]any{
			"message_id": updateID, "from": map[string]any{"id": claimantID, "username": username},
			"chat": map[string]any{"id": chatID, "type": chatType}, "text": challenge,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, callbackURL, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", signingSecret)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("submit Telegram claim: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var admission channelOnboardingClaimAdmission
	_ = json.Unmarshal(payload, &admission)
	admission.StatusCode = response.StatusCode
	admission.Body = strings.TrimSpace(string(payload))
	return admission
}

func requireChannelClaimDisposition(t *testing.T, label string, admission channelOnboardingClaimAdmission, disposition string) {
	t.Helper()
	if admission.StatusCode != http.StatusAccepted || admission.Disposition != disposition || len(admission.EventIDs) != 0 || len(admission.EventNames) != 0 {
		t.Fatalf("%s admission = %#v, want status 202 disposition %s and zero business events", label, admission, disposition)
	}
}

func assertChannelOnboardingClaimDidNotMutate(t *testing.T, label string, before, after channelonboarding.Result) {
	t.Helper()
	if before.Operation.Revision != after.Operation.Revision || before.Operation.Phase != after.Operation.Phase ||
		before.Operation.ActivationRevision != after.Operation.ActivationRevision || before.Operation.ConfirmationOperationID != after.Operation.ConfirmationOperationID ||
		before.Binding != nil || after.Binding != nil || before.IdentityOperation == nil || after.IdentityOperation == nil ||
		before.IdentityOperation.Revision != after.IdentityOperation.Revision || before.IdentityOperation.State != after.IdentityOperation.State {
		t.Fatalf("%s changed lifecycle state: before=%#v after=%#v", label, before, after)
	}
}

func assertNoChannelOnboardingActivation(t *testing.T, harness *channelOnboardingE2EHarness, operationID string) {
	t.Helper()
	for _, row := range readChannelOnboardingRows(t, harness.opts.ConfigPath, harness.endpoint) {
		if row.Operation != nil && row.Operation.OperationID == operationID && row.Activation != nil {
			t.Fatalf("operation %s unexpectedly retains activation: %#v", operationID, row)
		}
	}
}

func requireChannelOnboardingOperationRow(t *testing.T, harness *channelOnboardingE2EHarness, operationID string) channelOnboardingJourneyReadback {
	t.Helper()
	for _, row := range readChannelOnboardingRows(t, harness.opts.ConfigPath, harness.endpoint) {
		if row.Operation != nil && row.Operation.OperationID == operationID {
			return row
		}
	}
	t.Fatalf("channel list has no operation %s", operationID)
	return channelOnboardingJourneyReadback{}
}

type channelOnboardingCLICommand struct {
	stdout *lockedBuffer
	stderr *lockedBuffer
	done   chan int
}

func startChannelOnboardingCLICommand(t *testing.T, configPath, endpoint string, args []string, input string) channelOnboardingCLICommand {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	restore := installChannelOnboardingCLIInput(t, input)
	t.Cleanup(restore)
	commandArgs := append([]string{"--config", configPath}, args...)
	commandArgs = append(commandArgs, "--api-server", endpoint)
	go func() {
		done <- executeCLI(context.Background(), commandArgs, stdout, stderr, nil)
	}()
	return channelOnboardingCLICommand{stdout: stdout, stderr: stderr, done: done}
}

func requireChannelOnboardingCommandSuccess(t *testing.T, command channelOnboardingCLICommand) {
	t.Helper()
	select {
	case code := <-command.done:
		if code != 0 {
			t.Fatalf("channel command exited %d\nstdout:\n%s\nstderr:\n%s", code, command.stdout.String(), command.stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("channel command did not complete\nstdout:\n%s\nstderr:\n%s", command.stdout.String(), command.stderr.String())
	}
}

func installChannelOnboardingCLIInput(t *testing.T, contents string) func() {
	t.Helper()
	prior := os.Stdin
	input, err := os.CreateTemp(t.TempDir(), "channel-e2e-input-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := input.WriteString(contents); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	os.Stdin = input
	var restored bool
	return func() {
		if restored {
			return
		}
		restored = true
		os.Stdin = prior
		_ = input.Close()
	}
}

func (a channelOnboardingClaimAdmission) String() string {
	return fmt.Sprintf("status=%d disposition=%s events=%v/%v body=%q", a.StatusCode, a.Disposition, a.EventIDs, a.EventNames, a.Body)
}

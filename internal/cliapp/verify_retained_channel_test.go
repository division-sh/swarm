package cliapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func assertVerifyRetainedChannelPublicAdmission(t *testing.T, ctx context.Context, root, configPath string, owner any, db *sql.DB) {
	t.Helper()
	selected := owner.(interface {
		channelonboarding.Store
		channelonboarding.TeardownStore
		EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
	})
	now := time.Now().UTC()
	principal, err := selected.EnsureOperatorPrincipal(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := plangeneration.FromCanonicalValue(map[string]string{"test": "verify-retained-channel"})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := selected.ReserveChannelOnboarding(ctx, channelonboarding.StartRequest{
		OperationID: uuid.NewString(), RequestKeyHash: "verify-channel-key", RequestHash: "verify-channel-input", PrincipalID: principal.ID,
		Verb: channelonboarding.VerbConnect, Provider: "telegram",
		Interface: operatorchannel.InterfaceIdentity{
			InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
			ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:manifest", SemanticGeneration: "sha256:plan",
		},
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), BundleIdentity: "retained-channel-test",
			PackInventoryGeneration: "sha256:inventory", RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 2,
			PlanGeneration: generation, TargetGeneration: 3,
		},
		TargetSelector: "ingress:support/flow:telegram", Posture: channelonboarding.ActivationWebhookRegistration,
		Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge, SaveProof: true,
		CredentialReservations: []channelonboarding.CredentialReservation{{Role: "bot_token", StoreKey: "telegram_bot_token"}}, RequestedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err = selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: operation.OperationID, ExpectedRevision: operation.Revision, Phase: channelonboarding.PhaseCredentialsAdmitted,
		CredentialAdmissions: []channelonboarding.CredentialAdmission{{Role: "bot_token", StoreKey: "telegram_bot_token.retained",
			Kind: channelonboarding.CredentialAdmissionWritten, Receipt: "retained-receipt", ValueSeal: credentials.ValueSeal("credential-value-seal-v1:" + strings.Repeat("a", 64))}},
		ReplaceCredentialAdmissions: true, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseActivatingProvider, channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingOperatorConfirmation, channelonboarding.PhasePublishingActivation} {
		request := channelonboarding.AdvanceRequest{
			OperationID: operation.OperationID, ExpectedRevision: operation.Revision, Phase: phase,
			IdentityOperationID: uuid.NewString(), Now: now.Add(time.Duration(operation.Revision) * time.Second),
		}
		if phase == channelonboarding.PhaseAwaitingOperatorConfirmation || phase == channelonboarding.PhasePublishingActivation {
			request.BindingRevision = 1
		}
		operation, err = selected.AdvanceChannelOnboarding(ctx, request)
		if err != nil {
			t.Fatalf("seed retained onboarding phase %s: %v", phase, err)
		}
	}
	operation, activation, err := selected.PublishConnectedChannelActivation(ctx, channelonboarding.PublishActivationRequest{
		OperationID: operation.OperationID, ExpectedRevision: operation.Revision, ActivationID: uuid.NewString(), BindingRevision: 1,
		ConversationRef: "retained-conversation", ProofID: uuid.NewString(), ProofRevision: 1, Now: now.Add(8 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	teardown, err := selected.ReserveChannelTeardown(ctx, channelonboarding.ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: "verify-teardown-key", RequestHash: "verify-teardown-input",
		Kind: channelonboarding.TeardownContextRetirement, PrincipalID: principal.ID,
		Scope:       channelonboarding.TeardownScope{BundleHash: operation.Coordinate.BundleHash, ContextPublicationGeneration: operation.Coordinate.ContextPublicationGeneration},
		RequestedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"json", "quiet", "text"} {
		var out, diagnostic bytes.Buffer
		args := []string{"verify", root, "--config", configPath}
		if mode != "text" {
			args = append(args, "--"+mode)
		}
		if code := executeRootCommand(ctx, root, args, &out, &diagnostic); code != 0 {
			t.Fatalf("%s retained channel inspection: %d %s %s", mode, code, &out, &diagnostic)
		}
		if mode == "json" {
			var result verifyCommandResult
			if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			observed, obligated := false, false
			for _, observation := range result.Observations {
				observed = observed || observation.CheckID == "retained_channel_admission" && observation.Status == bootverify.AdmissionPassed
			}
			for _, obligation := range result.ExecutionObligations {
				obligated = obligated || obligation.ID == "connected_channel_teardown" && obligation.Subject == "channel-teardown:"+teardown.TeardownID
			}
			if !observed || !obligated || !result.AdmissionComplete || result.LiveReadiness != "not_evaluated" {
				t.Fatalf("durable rows became readiness or lost pending effects: %+v", result)
			}
		} else if !strings.Contains(out.String(), "startup not performed: connected_channel_teardown") {
			t.Fatalf("%s hid retained teardown: %s", mode, &out)
		}
	}
	actualOperation, err := selected.GetChannelOnboarding(ctx, operation.OperationID)
	if err != nil || !reflect.DeepEqual(operation, actualOperation) {
		t.Fatalf("verify reconciled onboarding: %+v %v", actualOperation, err)
	}
	actualActivation, err := selected.GetConnectedChannelActivation(ctx, activation.SlotKey)
	if err != nil || !reflect.DeepEqual(activation, actualActivation) {
		t.Fatalf("verify changed channel publication: %+v %v", actualActivation, err)
	}
	actualTeardown, err := selected.GetChannelTeardown(ctx, teardown.TeardownID)
	if err != nil || !reflect.DeepEqual(teardown, actualTeardown) {
		t.Fatalf("verify settled teardown: %+v %v", actualTeardown, err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE connected_channel_activations SET runtime_instance_id=$1 WHERE activation_id=$2", uuid.NewString(), activation.ActivationID); err != nil {
		t.Fatal(err)
	}
	if _, err := channelonboarding.InspectRetainedActivations(ctx, selected); err == nil || !strings.Contains(err.Error(), "exact owning onboarding operation") {
		t.Fatalf("boot's canonical durable ownership check missed crossed persisted evidence: %v", err)
	}
	var out, diagnostic bytes.Buffer
	if code := executeRootCommand(ctx, root, []string{"verify", root, "--config", configPath, "--json"}, &out, &diagnostic); code != cliExitConflict || !strings.Contains(out.String(), "exact owning onboarding operation") {
		t.Fatalf("public verify hid boot's crossed evidence refusal: %d %s %s", code, &out, &diagnostic)
	}
	if _, err := db.ExecContext(ctx, "UPDATE connected_channel_activations SET runtime_instance_id=$1 WHERE activation_id=$2", activation.Coordinate.RuntimeInstanceID, activation.ActivationID); err != nil {
		t.Fatal(err)
	}
}

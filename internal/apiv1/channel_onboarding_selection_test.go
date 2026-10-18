package apiv1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Any execution method on these absent collaborators panics: selection is
// strictly declaration readback, not a shortened provider setup journey.
type selectionExecutionRefusal struct {
	channelonboarding.ActivationRefresher
	channelonboarding.ConfirmationDispatcher
	channelonboarding.EffectRebindReconciler
	channelonboarding.ReadinessProjector
}

func TestChannelOnboardingSelectionReadOnlyHTTPBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected interface {
				channelonboarding.Store
				operatorchannel.Store
			}
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			identity := operatorchannel.InterfaceIdentity{InterfaceRef: operatorchannel.InterfaceHITLChannelV2,
				ChannelPackID: "provider.telegram.hitl_channel", ChannelPackVersion: "1.0.0",
				ChannelManifestHash: "sha256:" + strings.Repeat("a", 64), SemanticGeneration: "selection-contract"}.Normalized()
			proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			channels, err := operatorchannel.NewService(selected, proofs, operatorChannelAPICredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			principal, _, err := channels.Bootstrap(context.Background(), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			plan, err := plangeneration.FromCanonicalValue(map[string]string{"test": "selection-readback"})
			if err != nil {
				t.Fatal(err)
			}
			candidate := channelonboarding.Candidate{Provider: "telegram", Interface: identity,
				Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: "bundle-v2:sha256:" + strings.Repeat("b", 64),
					BundleIdentity: "support@1.0.0", PackInventoryGeneration: "inventory", RuntimeInstanceID: uuid.NewString(),
					ContextPublicationGeneration: 1, PlanGeneration: plan},
				Target: channelonboarding.CandidateTarget{Selector: "ingress:support:telegram", FlowPath: "support", Alias: "telegram",
					ServiceID: uuid.NewString(), Provider: "telegram", AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("selection"))},
				Posture: channelonboarding.ActivationWebhookRegistration, Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge,
				ProviderCredentialRole: "bot_token", SigningCredentialRole: "signing", ConfirmationOperation: "deliver"}
			catalog, err := channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{candidate})
			if err != nil {
				t.Fatal(err)
			}
			values, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			writer, err := channelonboarding.NewCredentialWriter(values)
			if err != nil {
				t.Fatal(err)
			}
			unreachable := selectionExecutionRefusal{}
			service, err := channelonboarding.NewService(channelonboarding.ServiceOptions{Store: selected, Identities: channels,
				Credentials: writer, Catalog: func() (*channelonboarding.CandidateCatalog, error) { return catalog, nil },
				Activations: unreachable, Confirmation: unreachable, Readiness: unreachable})
			if err != nil {
				t.Fatal(err)
			}
			handler := testHandler(t, Options{AuthTokens: []string{testToken}, OperatorPrincipalID: principal.ID,
				Handlers: ChannelOnboardingHandlers(ChannelOnboardingHandlerOptions{Onboarding: service, Channels: channels})})
			body := `{"jsonrpc":"2.0","id":"select","method":"channel.onboarding_select","params":{"verb":"reconnect","provider":"telegram"}}`
			unauthorized := httptest.NewRecorder()
			handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body)))
			if unauthorized.Code != http.StatusUnauthorized {
				t.Fatal("selection bypassed authentication", unauthorized.Code)
			}
			response := rpcCall(t, handler, body)
			if response.Error != nil {
				t.Fatal("authenticated declaration read failed", response.Error)
			}
			result := asMap(t, response.Result)
			if result["selection_fence"] == "" || asMap(t, result["candidate"])["provider_credential_role"] != "bot_token" {
				t.Fatal("selection omitted its exact requirement")
			}
			ops, err := selected.ListChannelOnboardingOperations(context.Background())
			if err != nil || len(ops) != 0 {
				t.Fatal("selection reserved durable work", err)
			}
			keys, err := values.List(context.Background())
			if err != nil || len(keys) != 0 {
				t.Fatal("selection touched credential storage", err)
			}
			for _, extra := range []string{`"provider_credential":"must-not-read"`, `"expected_selection":"foreign"`} {
				invalid := strings.Replace(body, `"provider":"telegram"`, `"provider":"telegram",`+extra, 1)
				if refused := rpcCall(t, handler, invalid); refused.Error == nil || refused.Error.Code != -32602 {
					t.Fatal("selection admitted mutation-only input", refused.Error)
				}
			}
		})
	}
}

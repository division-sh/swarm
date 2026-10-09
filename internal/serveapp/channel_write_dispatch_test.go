package serveapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	sessionexecution "github.com/division-sh/swarm/internal/sessionprovider/execution"
	"github.com/google/uuid"
)

func TestNativeChannelWriteDispatchNeverFallsBackToHTTP(t *testing.T) {
	generation, err := plangeneration.FromCanonicalValue(map[string]string{"test": "native-channel-dispatch"})
	if err != nil {
		t.Fatal(err)
	}
	operationID, effectID := uuid.NewString(), uuid.NewString()
	authority := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityChannelConfirmation, ID: effectID,
		ExecutionOwner: "channel-onboarding:" + operationID, LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
		FenceGeneration: 7, ExecutionMode: runtimeeffects.ExecutionModeLive,
		ChannelConfirmation: runtimeeffects.ChannelConfirmationAuthority{
			EffectOperationID: effectID, OnboardingOperationID: operationID, OnboardingRevision: 4,
			ActivationID: uuid.NewString(), ActivationRevision: 2, BindingRevision: 3, PrincipalID: uuid.NewString(),
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), BundleIdentity: "bundle:test@sha256:dispatch",
			PackInventoryGeneration: "sha256:dispatch", RuntimeInstanceID: uuid.NewString(),
			ContextPublicationGeneration: 7, PlanGeneration: generation, TargetGeneration: 1}}
	if !authority.Valid() {
		t.Fatal("invalid dispatch test authority")
	}
	ctx := runtimeeffects.WithAuthority(context.Background(), authority)
	tool := runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("provider_connector"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerInProcess),
		runtimecontracts.WithToolEffect(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
		runtimecontracts.WithToolInProcessTarget(runtimecontracts.ToolInProcessWhatsAppSendText))
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	httpExecutor := runtimeregistration.HTTPExecutor{Client: server.Client()}
	dispatcher := &serveChannelDeliveryDispatcher{sessionChannelWrites: map[string]sessionexecution.Channel{operationID: {}}}
	for _, row := range []struct {
		name        string
		operationID string
		credentials map[string]any
	}{
		{"reconstructed_handle", operationID, nil},
		{"missing_connection", uuid.NewString(), nil},
		{"credential_substitution", operationID, map[string]any{"token": "not-session-authority"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			_, err := executeChannelWrite(ctx, row.operationID, "deliver", "whatsapp.send_text", tool,
				map[string]any{"destination": "100000000001@s.whatsapp.net", "text": "never sent"}, row.credentials, nil,
				dispatcher.sessionChannelWrites[row.operationID], httpExecutor)
			if err == nil || requests.Load() != 0 {
				t.Fatalf("unowned native dispatch used an alternate transport: requests=%d err=%v", requests.Load(), err)
			}
		})
	}
}

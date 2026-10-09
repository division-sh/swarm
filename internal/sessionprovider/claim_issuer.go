package sessionprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/sessionprovider/input"
	"github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"
)

func prepareSessionClaim(ctx context.Context, admitted input.Admission, trigger providertriggers.InboundAdmissionPlan, channel packs.SatisfactionPlan) (authority.Claim, operatorchannel.InboundClaim, error) {
	var absent operatorchannel.InboundClaim
	generation, err := channel.Generation()
	if err != nil || !generation.Equal(admitted.Coordinate().PlanGeneration) {
		return authority.Claim{}, absent, fmt.Errorf("native claim requires its exact original channel plan")
	}
	var event capturedEvent
	if err := json.Unmarshal(admitted.OriginalCapture(), &event); err != nil {
		return authority.Claim{}, absent, err
	}
	if event.Scope.Kind != channelonboarding.SessionInputOnboarding || admitted.Scope() != channelonboarding.SessionInputOnboarding ||
		!event.Source.Coordinate.MatchesDeclaration(admitted.Coordinate()) || !event.ReceivedAt.Equal(admitted.ReceivedAt()) || !bytes.Equal(event.Body, admitted.Body()) {
		return authority.Claim{}, absent, fmt.Errorf("operator claim requires the original onboarding capture")
	}
	request, err := trigger.AdmitSessionInput(ctx, admitted)
	if err != nil {
		return authority.Claim{}, absent, err
	}
	delivery, err := trigger.ProjectDelivery(request)
	if err != nil {
		return authority.Claim{}, absent, err
	}
	providerID, err := event.PublicationProviderEventID()
	if err != nil {
		return authority.Claim{}, absent, err
	}
	fingerprint, err := event.PublicationFingerprint()
	if err != nil {
		return authority.Claim{}, absent, err
	}
	for _, output := range delivery.Events {
		fact, matched, err := channel.ProjectTextFact(string(output.Name), output.Authorization, output.Payload)
		if err != nil {
			return authority.Claim{}, absent, err
		}
		if !matched {
			continue
		}
		challenge, shaped := operatorchannel.ChallengeFromText(fact.Text)
		if !shaped {
			return authority.Claim{}, absent, fmt.Errorf("onboarding text is not an exact claim challenge")
		}
		claim := operatorchannel.InboundClaim{TextFact: fact, Provider: event.Scope.Session.Provider,
			ProviderEventID: providerID, PublicationID: operatorchannel.SessionClaimReceiptID(event.Scope.OnboardingOperation, providerID),
			Challenge: challenge, ProviderAuthorization: operatorchannel.Hash(output.Authorization.Provider(), output.Authorization.Event(),
				output.Authorization.PackID(), output.Authorization.PackVersion(), output.Authorization.ManifestHash(), output.Authorization.Generation().Diagnostic(), fingerprint)}
		if err := claim.Validate(); err != nil {
			return authority.Claim{}, absent, err
		}
		body, err := json.Marshal(claim)
		if err != nil {
			return authority.Claim{}, absent, err
		}
		return authorityfact.SealOwnedClaim(event.Scope.OnboardingOperation, event.Scope.OperationRevision,
			fingerprint, body, event.ReceivedAt, admitted.Context(), func() bool { return admitted.LifetimeCurrent(ctx) }), claim, nil
	}
	return authority.Claim{}, absent, fmt.Errorf("native claim has no admitted channel text mapping")
}

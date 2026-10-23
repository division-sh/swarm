package sessionprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/sessionprovider/input"
	"github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"
)

type sessionSetupProjection struct {
	claim    authority.Claim
	inbound  operatorchannel.InboundClaim
	nonClaim authority.NonClaim
}

func settleDeploymentClaim(ctx context.Context, identities operatorchannel.Store, op channelonboarding.Operation, claim operatorchannel.InboundClaim, valid func() bool) error {
	claim.PublicationID = operatorchannel.SessionClaimReceiptID(op.OperationID, claim.ProviderEventID)
	body, err := json.Marshal(claim)
	if err != nil {
		return err
	}
	sealed := authorityfact.SealOwnedClaim(op.OperationID, op.Revision, authorityfact.DeploymentParent,
		operatorchannel.Hash("telegram-registration-claim-v1", op.OperationID, string(body)), body, time.Now().UTC().Truncate(time.Microsecond), ctx, valid)
	_, err = identities.SettleSessionChannelClaim(ctx, sealed)
	return err
}

func prepareSessionSetup(ctx context.Context, admitted input.Admission, trigger providertriggers.InboundAdmissionPlan, channel packs.SatisfactionPlan) (sessionSetupProjection, error) {
	var absent sessionSetupProjection
	generation, err := channel.Generation()
	if err != nil || !admitted.Responsibility().DeploymentRegistration && !generation.Equal(admitted.Coordinate().PlanGeneration) {
		return absent, fmt.Errorf("native setup requires its exact original channel plan")
	}
	var event capturedEvent
	if err := json.Unmarshal(admitted.OriginalCapture(), &event); err != nil {
		return absent, err
	}
	if event.Scope.Kind != channelonboarding.SessionInputOnboarding || admitted.Scope() != channelonboarding.SessionInputOnboarding ||
		event.Scope.DeploymentRegistration != admitted.Responsibility().DeploymentRegistration ||
		(!event.Scope.DeploymentRegistration && !event.Source.Coordinate.MatchesDeclaration(admitted.Coordinate())) ||
		!event.ReceivedAt.Equal(admitted.ReceivedAt()) || !bytes.Equal(event.Body, admitted.Body()) {
		return absent, fmt.Errorf("operator setup requires the original onboarding capture")
	}
	request, err := trigger.AdmitSessionInput(ctx, admitted)
	if err != nil {
		return absent, err
	}
	delivery, err := trigger.ProjectDelivery(request)
	if err != nil {
		return absent, err
	}
	fact, selected, err := projectSessionSetupText(delivery, channel, event.Kind)
	if err != nil {
		return absent, err
	}
	challenge, shaped := operatorchannel.ChallengeFromText(fact.Text)
	current := func() bool { return admitted.LifetimeCurrent(ctx) }
	if event.Kind != "message" || !shaped {
		fingerprint, err := event.PublicationFingerprint()
		if err != nil {
			return absent, err
		}
		return sessionSetupProjection{nonClaim: authorityfact.SealOwnedNonClaim(event.Scope.OnboardingOperation,
			event.Scope.OperationRevision, fingerprint, admitted.OriginalCapture(), event.ReceivedAt, admitted.Context(), current,
			func(ctx context.Context) error {
				return admitted.Validate(ctx, event.Scope.Session.Provider, trigger.Generation())
			})}, nil
	}
	providerID, err := event.PublicationProviderEventID()
	if err != nil {
		return absent, err
	}
	fingerprint, err := event.PublicationFingerprint()
	if err != nil {
		return absent, err
	}
	claim := operatorchannel.InboundClaim{TextFact: fact, Provider: event.Scope.Session.Provider,
		ProviderEventID: providerID, PublicationID: operatorchannel.SessionClaimReceiptID(event.Scope.OnboardingOperation, providerID),
		Challenge: challenge, ProviderAuthorization: operatorchannel.Hash(selected.Authorization.Provider(), selected.Authorization.Event(),
			selected.Authorization.PackID(), selected.Authorization.PackVersion(), selected.Authorization.ManifestHash(), selected.Authorization.Generation().Diagnostic(), fingerprint)}
	if err := claim.Validate(); err != nil {
		return absent, err
	}
	body, err := json.Marshal(claim)
	if err != nil {
		return absent, err
	}
	scope := authority.RuntimeParent
	if event.Scope.DeploymentRegistration {
		scope = authority.DeploymentParent
	}
	return sessionSetupProjection{claim: authorityfact.SealOwnedClaim(event.Scope.OnboardingOperation, event.Scope.OperationRevision,
		scope, fingerprint, body, event.ReceivedAt, admitted.Context(), current), inbound: claim}, nil
}

func projectSessionSetupText(delivery providertriggers.Delivery, channel packs.SatisfactionPlan, kind string) (operatorchannel.TextFact, providertriggers.DeliveryEvent, error) {
	var fact operatorchannel.TextFact
	var selected providertriggers.DeliveryEvent
	matched := 0
	for _, output := range delivery.Events {
		if output.Kind != providertriggers.OutputKindNormalized {
			continue
		}
		text, ok, err := channel.ProjectTextFact(string(output.Name), output.Authorization, output.Payload)
		if err != nil {
			return fact, selected, err
		}
		if ok {
			matched++
			fact, selected = text, output
		}
	}
	if matched > 1 || kind == "message" && matched != 1 {
		return fact, selected, fmt.Errorf("native setup requires an unambiguous admitted channel text mapping")
	}
	return fact, selected, nil
}

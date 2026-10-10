package inboundpublication

import (
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
)

// OperatorProjection classifies input; it is not mutation or native admission.
type OperatorProjection struct {
	Claim         *operatorchannel.InboundClaim
	Action        *operatorchannel.InboundAction
	Text          *operatorchannel.InboundText
	BareCandidate *operatorchannel.InboundText
}

// HTTP and native ingress consume the same compiled channel mappings and text
// classification. Selected-store mutation still owns principal authorization.
func ProjectOperatorOutput(output providertriggers.DeliveryEvent, request Request, channelPlans []packs.SatisfactionPlan,
	selectBare func(operatorchannel.InboundText) (bool, error), previous *OperatorProjection,
) (*OperatorProjection, error) {
	if output.Kind != providertriggers.OutputKindNormalized {
		return previous, nil
	}
	for _, plan := range channelPlans {
		action, matched, err := plan.ProjectActionFact(string(output.Name), output.Authorization, output.Payload)
		if err != nil {
			return nil, err
		}
		if matched {
			if previous != nil {
				return nil, fmt.Errorf("normalized provider output ambiguously satisfies multiple operator channel interfaces")
			}
			previous = &OperatorProjection{Action: &operatorchannel.InboundAction{ActionFact: action,
				Provider: request.Provider, ProviderEventID: request.ProviderEventID, PublicationID: request.PublicationID,
				ProviderAuthorization: operatorOutputAuthorization(output)}}
		}
		text, matched, err := plan.ProjectTextFact(string(output.Name), output.Authorization, output.Payload)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		if previous != nil {
			return nil, fmt.Errorf("normalized provider output ambiguously satisfies multiple operator channel text interfaces")
		}
		previous, err = projectOperatorText(text, output, request, selectBare)
		if err != nil {
			return nil, err
		}
	}
	return previous, nil
}

func operatorOutputAuthorization(output providertriggers.DeliveryEvent) string {
	return operatorchannel.Hash(output.Authorization.Provider(), output.Authorization.Event(), output.Authorization.PackID(),
		output.Authorization.PackVersion(), output.Authorization.ManifestHash(), output.Authorization.Generation().Diagnostic())
}

func projectOperatorText(fact operatorchannel.TextFact, output providertriggers.DeliveryEvent, request Request,
	selectBare func(operatorchannel.InboundText) (bool, error),
) (*OperatorProjection, error) {
	challenge, shaped := operatorchannel.ChallengeFromText(fact.Text)
	if shaped {
		return &OperatorProjection{Claim: &operatorchannel.InboundClaim{TextFact: fact, Provider: request.Provider,
			ProviderEventID: request.ProviderEventID, PublicationID: request.PublicationID,
			Challenge: challenge, ProviderAuthorization: operatorOutputAuthorization(output)}}, nil
	}
	text := &operatorchannel.InboundText{TextFact: fact, Provider: request.Provider, ProviderEventID: request.ProviderEventID,
		PublicationID: request.PublicationID, ProviderAuthorization: operatorOutputAuthorization(output)}
	if fact.EntryReference != "" || fact.ReplyToReference != "" {
		return &OperatorProjection{Text: text}, nil
	}
	selected := false
	if selectBare != nil {
		var err error
		selected, err = selectBare(*text)
		if err != nil {
			return nil, err
		}
	}
	if selected {
		return &OperatorProjection{Text: text}, nil
	}
	return &OperatorProjection{BareCandidate: text}, nil
}

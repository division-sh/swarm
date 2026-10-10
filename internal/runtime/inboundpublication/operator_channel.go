package inboundpublication

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

// OperatorProjection classifies input; it is not mutation or native admission.
type OperatorProjection struct {
	Claim         *operatorchannel.InboundClaim
	Action        *operatorchannel.InboundAction
	Text          *operatorchannel.InboundText
	BareCandidate *operatorchannel.InboundText
	source        *operatorProjectionSource
}

type operatorProjectionSource struct {
	output         providertriggers.DeliveryEvent
	requestHash    string
	projectionHash string
}

type operatorCommitProof struct {
	source    operatorProjectionSource
	admission providertriggers.PublicationAdmission
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
			if err := previous.freezeSource(output, request); err != nil {
				return nil, err
			}
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
		if err := previous.freezeSource(output, request); err != nil {
			return nil, err
		}
	}
	return previous, nil
}

func (p *OperatorProjection) freezeSource(output providertriggers.DeliveryEvent, request Request) error {
	requestBytes, err := request.CanonicalBytes()
	if err != nil {
		return err
	}
	projection, err := canonicaljson.Bytes(p)
	if err != nil {
		return err
	}
	payload, err := canonicaljson.Bytes(output.Payload)
	if err != nil {
		return err
	}
	var frozen map[string]any
	if err := json.Unmarshal(payload, &frozen); err != nil {
		return err
	}
	output.Payload = frozen
	p.source = &operatorProjectionSource{output: output, requestHash: canonicaljson.HashBytes(requestBytes),
		projectionHash: canonicaljson.HashBytes(projection)}
	return nil
}

// NewOperatorCommit binds the compiled mapping to the original authenticated
// output. Exported facts remain readback, not a way to reconstruct permission.
func NewOperatorCommit(ctx context.Context, admission providertriggers.PublicationAdmission, request Request,
	evidence events.Event, projection *OperatorProjection,
) (CommitCommand, error) {
	if projection == nil || projection.source == nil || projection.BareCandidate != nil {
		return CommitCommand{}, fmt.Errorf("operator commit requires an owned operator projection")
	}
	command := CommitCommand{Admission: admission, Request: request, Finalization: Finalization{EvidenceEvent: evidence},
		OperatorChannelClaim: projection.Claim, OperatorChannelAction: projection.Action, OperatorChannelText: projection.Text,
		operator: &operatorCommitProof{source: *projection.source, admission: admission}}
	if err := command.Validate(); err != nil {
		return CommitCommand{}, err
	}
	if err := command.RequireOperatorAdmission(ctx); err != nil {
		return CommitCommand{}, err
	}
	return command, nil
}

func (c CommitCommand) validateOperatorProof() error {
	if c.operator == nil || !c.Admission.SameOwner(c.operator.admission) {
		return fmt.Errorf("operator publication requires its original projection and output owner")
	}
	request, err := c.Request.CanonicalBytes()
	if err != nil {
		return err
	}
	if canonicaljson.HashBytes(request) != c.operator.source.requestHash {
		return ErrRequestIdentityConflict
	}
	projection, err := canonicaljson.Bytes(&OperatorProjection{Claim: c.OperatorChannelClaim, Action: c.OperatorChannelAction, Text: c.OperatorChannelText})
	if err != nil || canonicaljson.HashBytes(projection) != c.operator.source.projectionHash {
		return fmt.Errorf("operator publication changed its compiled input projection")
	}
	return nil
}

func (c CommitCommand) RequireOperatorAdmission(ctx context.Context) error {
	if err := c.validateOperatorProof(); err != nil {
		return err
	}
	source, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found || source.BundleHash() != c.operator.admission.SourceBundleHash() {
		return fmt.Errorf("operator input no longer owns its admitted source")
	}
	return c.Admission.ValidateOperatorOutput(ctx, source.BundleHash(), c.Request.FlowPath, c.Request.Provider, c.Request.ProviderEventID, c.operator.source.output)
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

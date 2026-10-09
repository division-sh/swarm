package providertriggers

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	nativeinput "github.com/division-sh/swarm/internal/sessionprovider/input"
)

// PublicationAdmission transfers an authenticated plan's exact outputs. It is
// neither receiver permission nor a provider-delivery receipt identity.
type PublicationAdmission struct {
	bundleHash  string
	flowID      string
	provider    string
	outputs     []admittedPublicationOutput
	nativeInput *nativeinput.Admission
	generation  triggergeneration.Generation
}

type admittedPublicationOutput struct {
	name          events.EventType
	kind          provideroutput.Kind
	payloadDigest string
	authorization provideroutput.Authorization
}

func (p InboundAdmissionPlan) ProjectPublication(admitted AdmittedRequest, bundleHash, flowID string) (Delivery, PublicationAdmission, error) {
	if admitted.sessionInput != nil {
		if err := admitted.sessionInput.RequireBusiness(admitted.sessionInput.Context(), p.provider, p.generation); err != nil {
			return Delivery{}, PublicationAdmission{}, err
		}
		coordinate := admitted.sessionInput.Coordinate()
		target, err := packs.ParseChannelRegistrationTarget(admitted.sessionInput.TargetSelector())
		if err != nil || coordinate.Validate() != nil || coordinate.BundleHash != bundleHash || target.FlowPath != flowID || target.Provider != p.provider {
			return Delivery{}, PublicationAdmission{}, fmt.Errorf("session publication requires its exact executable source and target")
		}
	}
	if _, err := packs.IngressSubjectID(bundleHash, flowID, p.provider); err != nil {
		return Delivery{}, PublicationAdmission{}, err
	}
	delivery, err := p.ProjectDelivery(admitted)
	if err != nil {
		return Delivery{}, PublicationAdmission{}, err
	}
	if delivery.Response != nil || delivery.ProviderEventID != admitted.ProviderEventID() || delivery.ProviderEventType != admitted.ProviderEventType() {
		return Delivery{}, PublicationAdmission{}, fmt.Errorf("publication requires the exact admitted delivery, not a challenge or changed identity")
	}
	admission := PublicationAdmission{bundleHash: bundleHash, flowID: flowID, provider: p.provider,
		nativeInput: admitted.sessionInput, generation: p.generation}
	for _, output := range delivery.Events {
		payload, err := canonicaljson.Bytes(output.Payload)
		if err != nil {
			return Delivery{}, PublicationAdmission{}, err
		}
		admission.outputs = append(admission.outputs, admittedPublicationOutput{
			name: output.Name, kind: provideroutput.Kind(output.Kind),
			payloadDigest: canonicaljson.HashBytes(payload), authorization: output.Authorization,
		})
	}
	return delivery, admission, nil
}

func (a PublicationAdmission) ValidateOutput(bundleHash, provider string, ordinal, count int, event events.Event, kind provideroutput.Kind, authorization provideroutput.Authorization) error {
	if a.nativeInput != nil {
		if err := a.nativeInput.RequireBusiness(a.nativeInput.Context(), provider, a.generation); err != nil {
			return err
		}
		if event.RunID() != a.nativeInput.PublicationRunID() {
			return fmt.Errorf("native publication changed its admitted run")
		}
	}
	return a.validateOutput(bundleHash, provider, ordinal, count, event, kind, authorization)
}

// NativeInput transfers only the opaque native owner's product, not a writable
// authority record. HTTP admissions have no native responsibility to fence.
func (a PublicationAdmission) NativeInput() (nativeinput.Admission, bool) {
	if a.nativeInput == nil {
		return nativeinput.Admission{}, false
	}
	return *a.nativeInput, true
}

// ValidateCommitOutput is store-safe: selected responsibility is checked by its
// transaction owner, not by re-entering a root-store reader under SQL locks.
func (a PublicationAdmission) ValidateCommitOutput(ctx context.Context, bundleHash, provider string, ordinal, count int, event events.Event, kind provideroutput.Kind, authorization provideroutput.Authorization) error {
	if a.nativeInput != nil && (!a.nativeInput.LifetimeCurrent(ctx) || event.RunID() != a.nativeInput.PublicationRunID()) {
		return fmt.Errorf("native publication no longer owns its admitted lifetime and run")
	}
	return a.validateOutput(bundleHash, provider, ordinal, count, event, kind, authorization)
}

func (a PublicationAdmission) validateOutput(bundleHash, provider string, ordinal, count int, event events.Event, kind provideroutput.Kind, authorization provideroutput.Authorization) error {
	if a.bundleHash == "" || a.bundleHash != bundleHash || a.provider != provider || len(a.outputs) != count || ordinal < 0 || ordinal >= count {
		return fmt.Errorf("provider publication requires its exact authenticated output admission")
	}
	source := event.RoutingSource()
	if source.Kind() != events.RoutingSourceExternalIngress || source.Authority() != events.RoutingSourceAuthorityProviderAdmissionPlan ||
		source.Route() != (events.RouteIdentity{FlowID: a.flowID}) || event.EntityID() != "" || event.FlowInstance() != "" {
		return fmt.Errorf("provider publication contradicts its admitted declaration")
	}
	output := a.outputs[ordinal]
	if output.name != event.Type() || output.kind != kind || (output.authorization.Empty() != authorization.Empty()) ||
		(!authorization.Empty() && !output.authorization.Matches(authorization)) {
		return fmt.Errorf("provider publication changed its admitted output")
	}
	payload, err := canonicaljson.Decode(event.Payload())
	if err != nil {
		return err
	}
	encoded, err := canonicaljson.Bytes(payload)
	if err != nil || canonicaljson.HashBytes(encoded) != output.payloadDigest {
		return fmt.Errorf("provider publication changed its admitted payload")
	}
	return nil
}

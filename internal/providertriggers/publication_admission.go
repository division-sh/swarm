package providertriggers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	nativeinput "github.com/division-sh/swarm/internal/sessionprovider/input"
)

// IssuePrivateTestPublication seals an isolated test host's own provider
// double as the same raw/normalized pair consumed by inbound publication.
// The issuer is unavailable outside the private scenario host.
func (s *CatalogSnapshot) IssuePrivateTestPublication(ctx context.Context, bundleHash, flowID, providerEventID, eventName, operatorAccount string, payload map[string]any, now time.Time) (Delivery, PublicationAdmission, bool, error) {
	if !scenarioexecution.HasPrivateHostScope(ctx) {
		return Delivery{}, PublicationAdmission{}, false, fmt.Errorf("provider double requires its private test host")
	}
	if s == nil {
		return Delivery{}, PublicationAdmission{}, false, nil
	}
	for provider, entry := range s.byProvider {
		var rawName string
		matched := false
		for _, output := range entry.manifest.OutputManifest() {
			switch output.Kind {
			case OutputKindRaw:
				rawName = strings.TrimSpace(output.EventName.Literal)
			case OutputKindNormalized:
				matched = matched || output.Event == eventName
			}
		}
		if !matched {
			continue
		}
		if rawName == "" || providerEventID == "" || bundleHash == "" || flowID == "" {
			return Delivery{}, PublicationAdmission{}, true, fmt.Errorf("private provider output requires its exact raw declaration, receipt, source and flow")
		}
		authorization, err := provideroutput.NewAuthorization(provider, eventName, entry.identity.ID(), entry.identity.Version(), entry.identity.ManifestHash(), s.generation)
		if err != nil {
			return Delivery{}, PublicationAdmission{}, true, err
		}
		if err := s.VerifyProviderOutputAuthorization(authorization); err != nil {
			return Delivery{}, PublicationAdmission{}, true, err
		}
		providerEventType := strings.TrimPrefix(eventName, "inbound."+provider+".")
		raw := DeliveryEvent{Name: events.EventType(rawName), Kind: OutputKindRaw, Payload: map[string]any{
			"provider": provider, "event_type": providerEventType, "provider_event_type": providerEventType,
			"provider_event_id": providerEventID, "provider_delivery_id": providerEventID,
			"payload": payload, "headers": map[string]any{}, "received_at": now.UTC().Format(time.RFC3339),
		}}
		normalized := DeliveryEvent{Name: events.EventType(eventName), Kind: OutputKindNormalized, Payload: payload, Authorization: authorization}
		admission := PublicationAdmission{bundleHash: bundleHash, flowID: flowID, provider: provider, deliveryID: providerEventID, generation: s.generation, operatorAccount: operatorAccount, privateTest: true}
		for _, output := range []DeliveryEvent{raw, normalized} {
			encoded, err := canonicaljson.Bytes(output.Payload)
			if err != nil {
				return Delivery{}, PublicationAdmission{}, true, err
			}
			admission.outputs = append(admission.outputs, admittedPublicationOutput{
				name: output.Name, kind: provideroutput.Kind(output.Kind), payloadDigest: canonicaljson.HashBytes(encoded), authorization: output.Authorization,
			})
		}
		return Delivery{ProviderEventID: providerEventID, ProviderEventType: providerEventType, Events: []DeliveryEvent{raw, normalized}}, admission, true, nil
	}
	return Delivery{}, PublicationAdmission{}, false, nil
}

func (a PublicationAdmission) PrivateTestOutputMatches(eventName string, payload json.RawMessage) bool {
	if !a.privateTest || len(a.outputs) != 2 || a.outputs[1].name != events.EventType(eventName) {
		return false
	}
	decoded, err := canonicaljson.Decode(payload)
	if err != nil {
		return false
	}
	encoded, err := canonicaljson.Bytes(decoded)
	return err == nil && canonicaljson.HashBytes(encoded) == a.outputs[1].payloadDigest
}

// PublicationAdmission transfers an authenticated plan's exact outputs. It is
// not receiver permission. HTTP receipts consume its authenticated delivery ID;
// native receipt coordinates remain owned by the original capture.
type PublicationAdmission struct {
	privateTest     bool
	bundleHash      string
	flowID          string
	provider        string
	deliveryID      string
	outputs         []admittedPublicationOutput
	nativeInput     *nativeinput.Admission
	generation      triggergeneration.Generation
	operatorAccount string
}

type admittedPublicationOutput struct {
	name          events.EventType
	kind          provideroutput.Kind
	payloadDigest string
	authorization provideroutput.Authorization
}

func (a PublicationAdmission) SourceBundleHash() string { return a.bundleHash }
func (a PublicationAdmission) Provider() string         { return a.provider }
func (a PublicationAdmission) OperatorAccount() string  { return a.operatorAccount }

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
		deliveryID: delivery.ProviderEventID, nativeInput: admitted.sessionInput, generation: p.generation, operatorAccount: admitted.operatorAccount}
	for _, output := range delivery.Events {
		if admitted.sessionInput != nil && output.Kind == OutputKindNormalized {
			account, _ := output.Payload["operator_account"].(string)
			if admission.operatorAccount != "" && admission.operatorAccount != account {
				return Delivery{}, PublicationAdmission{}, fmt.Errorf("native outputs disagree on their receiving account")
			}
			admission.operatorAccount = account
		}
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
	if event.EntityID() != "" || event.FlowInstance() != "" {
		return fmt.Errorf("provider input cannot claim a concrete sender or receiver before route planning")
	}
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

// SameOwner compares the immutable output seal issued by ProjectPublication,
// not reconstructed provider coordinates or a caller's copy of output facts.
func (a PublicationAdmission) SameOwner(other PublicationAdmission) bool {
	return len(a.outputs) > 0 && len(a.outputs) == len(other.outputs) && &a.outputs[0] == &other.outputs[0]
}

// Operator input consumes one authenticated normalized output without publishing
// business events. SQL owners separately fence its selected native responsibility.
func (a PublicationAdmission) ValidateOperatorOutput(ctx context.Context, bundleHash, flowID, provider, receiptID string, output DeliveryEvent) error {
	if ctx == nil || ctx.Err() != nil || a.bundleHash == "" || a.bundleHash != bundleHash || a.flowID != flowID ||
		a.provider != provider || output.Kind != OutputKindNormalized {
		return fmt.Errorf("operator input requires its exact authenticated normalized output")
	}
	if a.nativeInput == nil && (a.deliveryID == "" || a.deliveryID != receiptID) {
		return fmt.Errorf("HTTP operator input changed its authenticated provider delivery identity")
	}
	if a.nativeInput != nil && !a.nativeInput.LifetimeCurrent(ctx) {
		return fmt.Errorf("operator input no longer owns its native lifetime")
	}
	payload, err := canonicaljson.Bytes(output.Payload)
	if err != nil {
		return err
	}
	matches := 0
	for _, admitted := range a.outputs {
		if admitted.name == output.Name && admitted.kind == provideroutput.KindNormalized &&
			!admitted.authorization.Empty() && admitted.authorization.Matches(output.Authorization) &&
			admitted.payloadDigest == canonicaljson.HashBytes(payload) {
			matches++
		}
	}
	if matches != 1 {
		return fmt.Errorf("operator input changed or ambiguously selected its admitted output")
	}
	return nil
}

// ValidateCommitOutput is store-safe: selected responsibility is checked by its
// transaction owner, not by re-entering a root-store reader under SQL locks.
func (a PublicationAdmission) ValidateCommitOutput(ctx context.Context, bundleHash, provider string, ordinal, count int, event events.Event, kind provideroutput.Kind, authorization provideroutput.Authorization) error {
	if (event.EntityID() != "" || event.FlowInstance() != "") && event.TargetRoute().Empty() {
		return fmt.Errorf("prepared provider output requires canonical receiver coordinates, not a concrete sender claim")
	}
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
		source.Route() != (events.RouteIdentity{FlowID: a.flowID}) || !event.SourceRoute().Empty() {
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

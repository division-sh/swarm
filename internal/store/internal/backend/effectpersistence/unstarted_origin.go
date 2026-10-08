package effectpersistence

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

type unstartedOriginEvidence struct {
	Version        int                                `json:"version"`
	Owner          flowidentity.RunScopedFlowInstance `json:"owner"`
	Kind           effects.CompletionOriginKind       `json:"kind"`
	DeliveryID     string                             `json:"delivery_id,omitempty"`
	RunID          string                             `json:"run_id,omitempty"`
	Route          string                             `json:"route,omitempty"`
	Token          string                             `json:"token,omitempty"`
	ClaimVersion   int64                              `json:"claim_version,omitempty"`
	AgentID        string                             `json:"agent_id,omitempty"`
	DirectiveID    string                             `json:"directive_id,omitempty"`
	DirectiveOwner string                             `json:"directive_owner,omitempty"`
}

func encodeUnstartedOrigin(origin effects.CompletionOrigin, owner flowidentity.RunScopedFlowInstance) ([]byte, error) {
	if err := origin.Validate(); err != nil {
		return nil, err
	}
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	evidence := unstartedOriginEvidence{Version: 1, Kind: origin.Kind, Owner: owner}
	switch origin.Kind {
	case effects.CompletionOriginDelivery:
		if origin.Delivery.SubscriberClass() != deliverylifecycle.SubscriberAgent || origin.Delivery.RunID() != owner.RunID {
			return nil, fmt.Errorf("unstarted turn requires an agent work origin")
		}
		evidence.DeliveryID, evidence.RunID = origin.Delivery.DeliveryID(), origin.Delivery.RunID()
		evidence.Route, evidence.Token = origin.Delivery.RouteIdentity(), origin.Delivery.PersistenceToken()
		evidence.ClaimVersion, evidence.AgentID = origin.Delivery.Version(), origin.Delivery.SubscriberID()
	case effects.CompletionOriginDirective:
		evidence.DirectiveID, evidence.DirectiveOwner = origin.Directive.OperationID, origin.Directive.ExecutionOwnerID
	}
	return json.Marshal(evidence)
}

func decodeUnstartedOrigin(raw []byte) (effects.CompletionOrigin, flowidentity.RunScopedFlowInstance, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var evidence unstartedOriginEvidence
	if err := decoder.Decode(&evidence); err != nil {
		return effects.CompletionOrigin{}, flowidentity.RunScopedFlowInstance{}, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF || evidence.Version != 1 {
		return effects.CompletionOrigin{}, flowidentity.RunScopedFlowInstance{}, fmt.Errorf("unstarted origin requires one versioned evidence record")
	}
	if err := evidence.Owner.Validate(); err != nil {
		return effects.CompletionOrigin{}, flowidentity.RunScopedFlowInstance{}, err
	}
	if evidence.Kind == effects.CompletionOriginDelivery && (evidence.DirectiveID != "" || evidence.DirectiveOwner != "") ||
		evidence.Kind == effects.CompletionOriginDirective && (evidence.DeliveryID != "" || evidence.RunID != "" || evidence.Route != "" || evidence.Token != "" || evidence.ClaimVersion != 0 || evidence.AgentID != "") {
		return effects.CompletionOrigin{}, flowidentity.RunScopedFlowInstance{}, fmt.Errorf("unstarted origin carries mixed work authority")
	}
	origin, err := decodeCompletionOrigin(string(evidence.Kind), evidence.DeliveryID, evidence.RunID, evidence.Route, evidence.Token, evidence.ClaimVersion, evidence.AgentID, evidence.DirectiveID, evidence.DirectiveOwner)
	if err == nil && origin.Kind == effects.CompletionOriginDelivery && origin.Delivery.RunID() != evidence.Owner.RunID {
		err = fmt.Errorf("unstarted origin changed its exact owner run")
	}
	return origin, evidence.Owner, err
}

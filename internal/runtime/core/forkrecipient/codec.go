package forkrecipient

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

type evidenceWire struct {
	SubscriberType string                   `json:"subscriber_type"`
	SubscriberID   string                   `json:"subscriber_id"`
	Path           string                   `json:"path,omitempty"`
	AgentPlan      *agentidentity.Plan      `json:"agent_plan,omitempty"`
	HandlerNode    *identity.ExecutableNode `json:"handler_node,omitempty"`
	HandlerEvent   events.EventType         `json:"handler_event"`
	Authority      authorityWire            `json:"authority"`
	RouteSource    string                   `json:"route_source,omitempty"`
}

type authorityWire struct {
	Kind        string `json:"kind"`
	PlanID      string `json:"plan_id,omitempty"`
	ReceiverPin string `json:"receiver_pin,omitempty"`
}

func (e Evidence) wire() evidenceWire {
	wire := evidenceWire{
		SubscriberType: e.Recipient.Code(), SubscriberID: e.Recipient.ID(),
		Path: e.Path, HandlerEvent: e.handlerEvent, RouteSource: e.routeSource,
		Authority: authorityWire{Kind: "local"},
	}
	if e.Recipient.IsAgent() {
		plan := e.AgentPlan
		wire.AgentPlan = &plan
	} else {
		node := e.handlerNode
		wire.HandlerNode = &node
	}
	if e.authority == authorityConnect {
		wire.Authority = authorityWire{
			Kind: "connect", PlanID: e.connectPlan.String(), ReceiverPin: e.receiverPin.String(),
		}
	}
	return wire
}

func (e Evidence) MarshalJSON() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e.normalized().wire())
}

func (e *Evidence) UnmarshalJSON(raw []byte) error {
	if e == nil {
		return fmt.Errorf("selected recipient destination is nil")
	}
	var wire evidenceWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return fmt.Errorf("decode selected recipient: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("decode selected recipient: expected exactly one JSON value")
	}
	in := Input{Path: wire.Path, HandlerEvent: wire.HandlerEvent, RouteSource: wire.RouteSource}
	switch wire.SubscriberType {
	case "node":
		node, err := identity.ParseExecutableNodeKey(wire.SubscriberID)
		if err != nil {
			return fmt.Errorf("decode selected recipient node: %w", err)
		}
		in.Recipient, err = events.NewNodeDeliveryRecipient(node)
		if err != nil {
			return err
		}
		if wire.HandlerNode == nil || wire.AgentPlan != nil {
			return fmt.Errorf("selected node requires handler_node and forbids agent_plan")
		}
		in.HandlerNode = *wire.HandlerNode
	case "agent":
		var err error
		in.Recipient, err = events.NewAgentDeliveryRecipient(wire.SubscriberID)
		if err != nil {
			return err
		}
		if wire.AgentPlan == nil || wire.HandlerNode != nil {
			return fmt.Errorf("selected agent requires agent_plan and forbids handler_node")
		}
		in.AgentPlan = *wire.AgentPlan
	default:
		return fmt.Errorf("selected recipient kind %q is invalid", wire.SubscriberType)
	}
	var (
		decoded Evidence
		err     error
	)
	switch wire.Authority.Kind {
	case "local":
		if wire.Authority.PlanID != "" || wire.Authority.ReceiverPin != "" {
			return fmt.Errorf("local selected recipient cannot carry connect authority")
		}
		decoded, err = NewLocal(in)
	case "connect":
		plan, planErr := decodeDigest(wire.Authority.PlanID)
		if planErr != nil {
			return fmt.Errorf("selected connect plan: %w", planErr)
		}
		pin, pinErr := decodeDigest(wire.Authority.ReceiverPin)
		if pinErr != nil {
			return fmt.Errorf("selected receiver pin: %w", pinErr)
		}
		decoded, err = NewConnect(in, events.AdmitConnectPlanIdentity(plan), events.AdmitConnectReceiverIdentity(pin))
	default:
		return fmt.Errorf("selected recipient authority %q is invalid", wire.Authority.Kind)
	}
	if err != nil {
		return err
	}
	*e = decoded
	return nil
}

func decodeDigest(raw string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != len(digest) || hex.EncodeToString(decoded) != raw {
		return digest, fmt.Errorf("identity requires a canonical SHA-256 digest")
	}
	copy(digest[:], decoded)
	if digest == [sha256.Size]byte{} {
		return digest, fmt.Errorf("identity digest must not be zero")
	}
	return digest, nil
}

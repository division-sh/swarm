// Package forkrecipient owns runless selected-recipient evidence. It does not
// derive routes, reconstruct historical recipients, or admit execution contexts.
package forkrecipient

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

type authorityKind uint8

const (
	authorityLocal authorityKind = iota + 1
	authorityConnect
)

// Evidence is a selected declaration blueprint, not historical or live authority.
// Use Key or Equal for semantic identity: diagnostic provenance is not identity.
type Evidence struct {
	Recipient events.DeliveryRecipient
	Path      string
	AgentPlan agentidentity.Plan

	handlerNode  identity.ExecutableNode
	handlerEvent events.EventType
	authority    authorityKind
	connectPlan  events.ConnectPlanIdentity
	receiverPin  events.ConnectReceiverIdentity
	routeSource  string
}

// Input must come from the selected source's existing recipient/handler owners.
// RouteSource is preserved only for diagnostics.
type Input struct {
	Recipient    events.DeliveryRecipient
	Path         string
	AgentPlan    agentidentity.Plan
	HandlerNode  identity.ExecutableNode
	HandlerEvent events.EventType
	RouteSource  string
}

func NewLocal(in Input) (Evidence, error) {
	return newEvidence(in, authorityLocal, events.ConnectPlanIdentity{}, events.ConnectReceiverIdentity{})
}

// NewConnect retains the plan/pin association supplied by the compiled owner.
// Digest presence is not proof of graph membership; that owner admits the pair.
func NewConnect(in Input, plan events.ConnectPlanIdentity, pin events.ConnectReceiverIdentity) (Evidence, error) {
	return newEvidence(in, authorityConnect, plan, pin)
}

func newEvidence(in Input, authority authorityKind, plan events.ConnectPlanIdentity, pin events.ConnectReceiverIdentity) (Evidence, error) {
	e := Evidence{
		Recipient: in.Recipient, Path: in.Path, AgentPlan: in.AgentPlan,
		handlerNode: in.HandlerNode, handlerEvent: in.HandlerEvent,
		authority: authority, connectPlan: plan, receiverPin: pin,
		routeSource: in.RouteSource,
	}.normalized()
	if err := e.Validate(); err != nil {
		return Evidence{}, err
	}
	return e, nil
}

func (e Evidence) normalized() Evidence {
	e.Path = strings.TrimSpace(e.Path)
	e.AgentPlan = e.AgentPlan.Normalize()
	e.routeSource = strings.TrimSpace(e.routeSource)
	return e
}

func (e Evidence) Validate() error {
	e = e.normalized()
	if strings.ContainsAny(e.Path, "\x00\r\n") {
		return fmt.Errorf("selected recipient path contains control characters")
	}
	if !eventidentity.IsCanonicalName(string(e.handlerEvent)) {
		return fmt.Errorf("selected recipient requires an exact handler event")
	}
	switch {
	case e.Recipient.IsNode():
		node, _ := e.Recipient.Node()
		if !e.handlerNode.Valid() || !node.Equal(e.handlerNode) {
			return fmt.Errorf("selected node recipient requires its exact handler owner")
		}
		if !e.AgentPlan.IsZero() {
			return fmt.Errorf("selected node recipient cannot carry an agent plan")
		}
	case e.Recipient.IsAgent():
		if !e.handlerNode.Empty() {
			return fmt.Errorf("selected agent recipient cannot carry a node handler")
		}
		if err := e.AgentPlan.Validate(); err != nil {
			return fmt.Errorf("selected agent recipient plan: %w", err)
		}
		if e.AgentPlan.AgentID() != e.Recipient.ID() {
			return fmt.Errorf("selected agent plan does not match its recipient")
		}
	default:
		return fmt.Errorf("selected recipient requires an admitted node or agent")
	}
	switch e.authority {
	case authorityLocal:
		if !e.connectPlan.Empty() || !e.receiverPin.Empty() {
			return fmt.Errorf("local selected recipient cannot carry connect authority")
		}
	case authorityConnect:
		if e.connectPlan.Empty() || e.receiverPin.Empty() {
			return fmt.Errorf("connected selected recipient requires both plan and receiver pin identities")
		}
	default:
		return fmt.Errorf("selected recipient requires local or connect authority")
	}
	return nil
}

func (e Evidence) RouteSourceCode() string              { return e.routeSource }
func (e Evidence) HandlerNode() identity.ExecutableNode { return e.handlerNode }
func (e Evidence) HandlerEvent() events.EventType       { return e.handlerEvent }
func (e Evidence) Connect() (events.ConnectPlanIdentity, events.ConnectReceiverIdentity, bool) {
	return e.connectPlan, e.receiverPin, e.authority == authorityConnect
}

// Key is the complete comparable identity of validated, normalized evidence.
// Its zero value does not represent an admitted recipient.
type Key struct {
	recipient    events.DeliveryRecipient
	path         string
	agentPlan    agentidentity.Plan
	handlerNode  identity.ExecutableNode
	handlerEvent events.EventType
	authority    authorityKind
	connectPlan  events.ConnectPlanIdentity
	receiverPin  events.ConnectReceiverIdentity
}

func (e Evidence) Key() (Key, error) {
	e = e.normalized()
	if err := e.Validate(); err != nil {
		return Key{}, err
	}
	return Key{
		recipient: e.Recipient, path: e.Path, agentPlan: e.AgentPlan,
		handlerNode: e.handlerNode, handlerEvent: e.handlerEvent,
		authority: e.authority, connectPlan: e.connectPlan, receiverPin: e.receiverPin,
	}, nil
}

func Equal(left, right Evidence) (bool, error) {
	l, err := left.Key()
	if err != nil {
		return false, fmt.Errorf("left selected recipient: %w", err)
	}
	r, err := right.Key()
	if err != nil {
		return false, fmt.Errorf("right selected recipient: %w", err)
	}
	return l == r, nil
}

func Less(left, right Evidence) (bool, error) {
	l, err := left.Key()
	if err != nil {
		return false, fmt.Errorf("left selected recipient: %w", err)
	}
	r, err := right.Key()
	if err != nil {
		return false, fmt.Errorf("right selected recipient: %w", err)
	}
	return compareKeys(l, r) < 0, nil
}

func compareKeys(left, right Key) int {
	for _, pair := range [][2]string{
		{left.recipient.Code(), right.recipient.Code()},
		{left.recipient.ID(), right.recipient.ID()},
		{left.path, right.path},
	} {
		if order := cmp.Compare(pair[0], pair[1]); order != 0 {
			return order
		}
	}
	if left.agentPlan != right.agentPlan {
		if agentidentity.LessPlan(left.agentPlan, right.agentPlan) {
			return -1
		}
		return 1
	}
	for _, pair := range [][2]string{
		{left.handlerNode.Key(), right.handlerNode.Key()},
		{string(left.handlerEvent), string(right.handlerEvent)},
	} {
		if order := cmp.Compare(pair[0], pair[1]); order != 0 {
			return order
		}
	}
	if order := cmp.Compare(left.authority, right.authority); order != 0 {
		return order
	}
	if order := cmp.Compare(left.connectPlan.String(), right.connectPlan.String()); order != 0 {
		return order
	}
	return cmp.Compare(left.receiverPin.String(), right.receiverPin.String())
}

// Fingerprint is a one-way semantic projection, not a decoder or live binding.
func (e Evidence) Fingerprint() (string, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}
	wire := e.normalized().wire()
	wire.RouteSource = ""
	raw, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("encode selected recipient fingerprint: %w", err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// CanonicalSet validates every member before coalescing exact duplicates. It
// does not replace receiver-pin or execution-slot collision admission.
func CanonicalSet(in []Evidence) ([]Evidence, error) {
	byKey := make(map[Key]Evidence, len(in))
	for index, e := range in {
		e = e.normalized()
		key, err := e.Key()
		if err != nil {
			return nil, fmt.Errorf("selected recipient %d: %w", index, err)
		}
		// Choose a deterministic diagnostic representative, never a semantic key.
		if previous, exists := byKey[key]; !exists || e.routeSource < previous.routeSource {
			byKey[key] = e
		}
	}
	keys := make([]Key, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, compareKeys)
	if len(keys) == 0 {
		return nil, nil
	}
	out := make([]Evidence, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out, nil
}

// Satisfies checks exact evidence agreement and child-run agent composition.
// The caller owns actual-route projection, node/event run binding, and canonical
// target, claim, payload and reply correspondence before calling this method.
func (e Evidence) Satisfies(childRun string, actual Evidence, live agentidentity.Identity) error {
	if strings.TrimSpace(childRun) == "" {
		return fmt.Errorf("selected recipient satisfaction requires an admitted child run")
	}
	equal, err := Equal(e, actual)
	if err != nil {
		return err
	}
	if !equal {
		return fmt.Errorf("actual recipient does not satisfy selected evidence")
	}
	if e.Recipient.IsNode() {
		if !live.IsZero() {
			return fmt.Errorf("selected node recipient cannot carry live agent identity")
		}
		return nil
	}
	expected, err := e.AgentPlan.Live(childRun)
	if err != nil {
		return fmt.Errorf("bind selected agent plan: %w", err)
	}
	equal, err = agentidentity.Equal(expected, live)
	if err != nil {
		return fmt.Errorf("actual live agent identity: %w", err)
	}
	if !equal {
		return fmt.Errorf("actual agent does not match selected plan in the child run")
	}
	return nil
}

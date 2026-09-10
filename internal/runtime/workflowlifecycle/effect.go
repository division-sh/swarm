package workflowlifecycle

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

type Kind uint8

const (
	KindUnknown Kind = iota
	KindInitialEntry
	KindAcceptedEvent
)

type Transition struct {
	from         string
	to           string
	id           string
	compiled     *contracts.CompiledTransition
	selection    handlerselection.HandlerRuleSelectionFact
	guards       []string
	guardNode    identity.ExecutableNode
	guardHandler string
	guardName    string
}

func (t Transition) From() string { return t.from }
func (t Transition) To() string   { return t.to }
func (t Transition) ID() string   { return t.id }

type Effect struct {
	kind          Kind
	route         runtimeflowidentity.Route
	entityID      identity.EntityID
	stage         string
	eventID       string
	eventType     string
	executionMode executionmode.Mode
	occurredAt    time.Time
	transition    *Transition
}

func NewInitialEntry(route runtimeflowidentity.Route, entityID identity.EntityID, stage string, mode executionmode.Mode, occurredAt time.Time) (Effect, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	effect := Effect{
		kind:          KindInitialEntry,
		route:         route,
		entityID:      identity.NormalizeEntityID(entityID.String()),
		stage:         strings.TrimSpace(stage),
		executionMode: mode,
		occurredAt:    occurredAt.UTC(),
	}
	if !effect.route.Valid() || effect.entityID.IsZero() || effect.stage == "" || !effect.executionMode.Valid() || effect.occurredAt.IsZero() {
		return Effect{}, fmt.Errorf("initial workflow entry requires instance, stage, execution mode, and exact occurrence time")
	}
	return effect, nil
}

func NewAcceptedEvent(route runtimeflowidentity.Route, entityID identity.EntityID, eventID, eventType string, mode executionmode.Mode, occurredAt time.Time, transition *Transition) (Effect, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	effect := Effect{
		kind:          KindAcceptedEvent,
		route:         route,
		entityID:      identity.NormalizeEntityID(entityID.String()),
		eventID:       strings.TrimSpace(eventID),
		eventType:     strings.TrimSpace(eventType),
		executionMode: mode,
		occurredAt:    occurredAt.UTC(),
	}
	if !effect.route.Valid() || effect.entityID.IsZero() || effect.eventID == "" || effect.eventType == "" || !effect.executionMode.Valid() || effect.occurredAt.IsZero() {
		return Effect{}, fmt.Errorf("accepted workflow event requires instance and exact event identity")
	}
	if transition != nil {
		value := *transition
		if err := value.Validate(); err != nil {
			return Effect{}, err
		}
		effect.transition = &value
	}
	return effect, nil
}

func (e Effect) Kind() Kind                        { return e.kind }
func (e Effect) Route() runtimeflowidentity.Route  { return e.route }
func (e Effect) EntityID() identity.EntityID       { return e.entityID }
func (e Effect) InitialStage() string              { return e.stage }
func (e Effect) EventID() string                   { return e.eventID }
func (e Effect) EventType() string                 { return e.eventType }
func (e Effect) ExecutionMode() executionmode.Mode { return e.executionMode }
func (e Effect) OccurredAt() time.Time             { return e.occurredAt }

func (e Effect) Transition() (Transition, bool) {
	if e.transition == nil {
		return Transition{}, false
	}
	return *e.transition, true
}

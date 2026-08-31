package agentidentity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

type NameSource string

const (
	NameSourceDeclared       NameSource = "declared"
	NameSourceRuntimeCreated NameSource = "runtime_created"
)

type Name struct {
	AgentID string     `json:"agent_id"`
	Owner   string     `json:"owner"`
	Source  NameSource `json:"source"`
}

func DeclaredName(agentID, owner string) (Name, error) {
	return newName(agentID, owner, NameSourceDeclared)
}

func RuntimeName(agentID, owner string) (Name, error) {
	return newName(agentID, owner, NameSourceRuntimeCreated)
}

func newName(agentID, owner string, source NameSource) (Name, error) {
	name := Name{
		AgentID: strings.TrimSpace(agentID),
		Owner:   strings.TrimSpace(owner),
		Source:  NameSource(strings.TrimSpace(string(source))),
	}
	if err := name.Validate(); err != nil {
		return Name{}, err
	}
	return name, nil
}

func (n Name) Normalize() Name {
	n.AgentID = strings.TrimSpace(n.AgentID)
	n.Owner = strings.TrimSpace(n.Owner)
	n.Source = NameSource(strings.TrimSpace(string(n.Source)))
	return n
}

func (n Name) Validate() error {
	n = n.Normalize()
	if n.AgentID == "" {
		return fmt.Errorf("agent identity agent_id is required")
	}
	if n.Owner == "" {
		return fmt.Errorf("agent identity name owner is required")
	}
	switch n.Source {
	case NameSourceDeclared, NameSourceRuntimeCreated:
		return nil
	default:
		return fmt.Errorf("invalid agent identity name source %q", n.Source)
	}
}

type RoutePresence string

const (
	RoutePresent RoutePresence = "present"
	RouteRoot    RoutePresence = "root"
)

type Route struct {
	Presence     RoutePresence `json:"presence"`
	ScopeKey     string        `json:"scope_key"`
	InstanceID   string        `json:"instance_id"`
	InstancePath string        `json:"instance_path"`
}

func PresentRoute(scopeKey, instanceID, instancePath string) (Route, error) {
	value := Route{
		Presence:     RoutePresent,
		ScopeKey:     scopeKey,
		InstanceID:   instanceID,
		InstancePath: instancePath,
	}
	value = value.Normalize()
	if err := value.Validate(); err != nil {
		return Route{}, err
	}
	return value, nil
}

func RootRoute() Route {
	return Route{Presence: RouteRoot}
}

func (r Route) Normalize() Route {
	r.Presence = RoutePresence(strings.TrimSpace(string(r.Presence)))
	r.ScopeKey = strings.Trim(strings.TrimSpace(r.ScopeKey), "/")
	r.InstanceID = strings.TrimSpace(r.InstanceID)
	r.InstancePath = strings.Trim(strings.TrimSpace(r.InstancePath), "/")
	return r
}

func (r Route) Validate() error {
	r = r.Normalize()
	switch r.Presence {
	case RouteRoot:
		if r.ScopeKey != "" || r.InstanceID != "" || r.InstancePath != "" {
			return fmt.Errorf("root agent identity route must not carry flow route fields")
		}
		return nil
	case RoutePresent:
		if r.ScopeKey == "" || r.InstanceID == "" || r.InstancePath == "" {
			return fmt.Errorf("present agent identity route is incomplete")
		}
		return nil
	default:
		return fmt.Errorf("invalid agent identity route presence %q", r.Presence)
	}
}

func (r Route) Fields() (scopeKey, instanceID, instancePath string, present bool) {
	r = r.Normalize()
	if r.Presence != RoutePresent {
		return "", "", "", false
	}
	return r.ScopeKey, r.InstanceID, r.InstancePath, true
}

type Identity struct {
	RunID string `json:"run_id"`
	Name  Name   `json:"name"`
	Route Route  `json:"route"`
}

// Plan is declaration-owned, runless agent routing data. It may appear in
// compiled blueprints, but it is not live authority and cannot address store
// or process state until composed with an admitted run.
type Plan struct {
	Name  Name  `json:"name"`
	Route Route `json:"route"`
}

type StorageFields struct {
	RunID            string
	AgentID          string
	NameOwner        string
	NameSource       string
	RoutePresence    string
	FlowScopeKey     string
	FlowInstanceID   string
	FlowInstancePath string
}

func New(runID string, name Name, route Route) (Identity, error) {
	identity := Identity{RunID: strings.TrimSpace(runID), Name: name.Normalize(), Route: route.Normalize()}
	if err := identity.Validate(); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func NewPlan(name Name, route Route) (Plan, error) {
	plan := Plan{Name: name.Normalize(), Route: route.Normalize()}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (p Plan) Normalize() Plan {
	p.Name = p.Name.Normalize()
	p.Route = p.Route.Normalize()
	return p
}

func (p Plan) Validate() error {
	p = p.Normalize()
	if err := p.Name.Validate(); err != nil {
		return err
	}
	return p.Route.Validate()
}

func (p Plan) IsZero() bool {
	return p == Plan{}
}

func (p Plan) AgentID() string {
	return p.Normalize().Name.AgentID
}

func (p Plan) FlowInstance() string {
	return p.Normalize().Route.InstancePath
}

func (p Plan) Description() string {
	p = p.Normalize()
	if p.Route.Presence == RouteRoot {
		return fmt.Sprintf("%s (planned root, owner=%s)", p.Name.AgentID, p.Name.Owner)
	}
	return fmt.Sprintf("%s (planned flow_instance=%s, owner=%s)", p.Name.AgentID, p.Route.InstancePath, p.Name.Owner)
}

// Fingerprint is the one-way physical projection of a declaration-owned
// blueprint. It intentionally excludes run identity because a Plan exists
// before admission to a concrete run.
func (p Plan) Fingerprint() (string, error) {
	p = p.Normalize()
	if err := p.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encode agent plan fingerprint: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func LessPlan(left, right Plan) bool {
	left = left.Normalize()
	right = right.Normalize()
	leftParts := [...]string{
		left.Name.AgentID,
		left.Name.Owner,
		string(left.Name.Source),
		string(left.Route.Presence),
		left.Route.ScopeKey,
		left.Route.InstanceID,
		left.Route.InstancePath,
	}
	rightParts := [...]string{
		right.Name.AgentID,
		right.Name.Owner,
		string(right.Name.Source),
		string(right.Route.Presence),
		right.Route.ScopeKey,
		right.Route.InstanceID,
		right.Route.InstancePath,
	}
	for idx := range leftParts {
		if leftParts[idx] != rightParts[idx] {
			return leftParts[idx] < rightParts[idx]
		}
	}
	return false
}

func (p Plan) Live(runID string) (Identity, error) {
	p = p.Normalize()
	if err := p.Validate(); err != nil {
		return Identity{}, err
	}
	return New(runID, p.Name, p.Route)
}

func (i Identity) Plan() (Plan, error) {
	i = i.Normalize()
	if err := i.Validate(); err != nil {
		return Plan{}, err
	}
	return NewPlan(i.Name, i.Route)
}

func (i Identity) Normalize() Identity {
	i.RunID = strings.TrimSpace(i.RunID)
	i.Name = i.Name.Normalize()
	i.Route = i.Route.Normalize()
	return i
}

func (i Identity) Validate() error {
	i = i.Normalize()
	if i.RunID == "" {
		return fmt.Errorf("agent identity run_id is required")
	}
	if err := i.Name.Validate(); err != nil {
		return err
	}
	if err := i.Route.Validate(); err != nil {
		return err
	}
	return nil
}

func (i Identity) IsZero() bool {
	return i == Identity{}
}

func (i Identity) AgentID() string {
	return i.Normalize().Name.AgentID
}

func (i Identity) FlowInstance() string {
	return i.Normalize().Route.InstancePath
}

func (i Identity) MatchesAgentID(agentID string) bool {
	return i.AgentID() == strings.TrimSpace(agentID)
}

// Equal compares two complete, validated live identities. Callers must not
// infer identity equality from the authored agent ID or route alone.
func Equal(left, right Identity) (bool, error) {
	left = left.Normalize()
	if err := left.Validate(); err != nil {
		return false, fmt.Errorf("left agent identity: %w", err)
	}
	right = right.Normalize()
	if err := right.Validate(); err != nil {
		return false, fmt.Errorf("right agent identity: %w", err)
	}
	return left == right, nil
}

func (i Identity) StorageFields() (StorageFields, error) {
	i = i.Normalize()
	if err := i.Validate(); err != nil {
		return StorageFields{}, err
	}
	return StorageFields{
		RunID:            i.RunID,
		AgentID:          i.Name.AgentID,
		NameOwner:        i.Name.Owner,
		NameSource:       string(i.Name.Source),
		RoutePresence:    string(i.Route.Presence),
		FlowScopeKey:     i.Route.ScopeKey,
		FlowInstanceID:   i.Route.InstanceID,
		FlowInstancePath: i.Route.InstancePath,
	}, nil
}

func FromStorageFields(fields StorageFields) (Identity, error) {
	return New(
		fields.RunID,
		Name{
			AgentID: fields.AgentID,
			Owner:   fields.NameOwner,
			Source:  NameSource(fields.NameSource),
		},
		Route{
			Presence:     RoutePresence(fields.RoutePresence),
			ScopeKey:     fields.FlowScopeKey,
			InstanceID:   fields.FlowInstanceID,
			InstancePath: fields.FlowInstancePath,
		},
	)
}

func (i Identity) Description() string {
	i = i.Normalize()
	if i.Route.Presence == RouteRoot {
		return fmt.Sprintf("%s (run=%s, root, owner=%s)", i.Name.AgentID, i.RunID, i.Name.Owner)
	}
	return fmt.Sprintf(
		"%s (run=%s, flow_instance=%s, scope_key=%s, instance_id=%s, owner=%s)",
		i.Name.AgentID,
		i.RunID,
		i.Route.InstancePath,
		i.Route.ScopeKey,
		i.Route.InstanceID,
		i.Name.Owner,
	)
}

func Less(left, right Identity) bool {
	left = left.Normalize()
	right = right.Normalize()
	leftParts := [...]string{
		left.RunID,
		left.Name.AgentID,
		left.Name.Owner,
		string(left.Name.Source),
		string(left.Route.Presence),
		left.Route.ScopeKey,
		left.Route.InstanceID,
		left.Route.InstancePath,
	}
	rightParts := [...]string{
		right.RunID,
		right.Name.AgentID,
		right.Name.Owner,
		string(right.Name.Source),
		string(right.Route.Presence),
		right.Route.ScopeKey,
		right.Route.InstanceID,
		right.Route.InstancePath,
	}
	for idx := range leftParts {
		if leftParts[idx] != rightParts[idx] {
			return leftParts[idx] < rightParts[idx]
		}
	}
	return false
}

// Fingerprint is a one-way physical projection for names that cannot carry the
// typed identity directly. It is never parsed and is not a public identifier.
func (i Identity) Fingerprint() (string, error) {
	i = i.Normalize()
	if err := i.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(i)
	if err != nil {
		return "", fmt.Errorf("encode agent identity fingerprint: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

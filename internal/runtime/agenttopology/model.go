package agenttopology

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/google/uuid"
)

type AuthorityKind string

const (
	AuthorityStaticDeclarationPlan AuthorityKind = "static_declaration_plan"
	AuthorityFlowReadinessPlan     AuthorityKind = "flow_readiness_plan"
	AuthorityEphemeralExecution    AuthorityKind = "ephemeral_execution"
)

type ExecutionLifetime string

const (
	LifetimeDurableManaged ExecutionLifetime = "durable_managed"
	LifetimeEphemeral      ExecutionLifetime = "ephemeral"
)

type StaticDeclarationPlan struct {
	SourceSetRevision string `json:"source_set_revision"`
	BundleHash        string `json:"bundle_hash"`
	BundleSource      string `json:"bundle_source"`
}

type FlowReadinessPlan struct {
	RunID           string `json:"run_id"`
	InstancePath    string `json:"instance_path"`
	PlanFingerprint string `json:"plan_fingerprint"`
}

type EphemeralExecution struct {
	ExecutionID string `json:"execution_id"`
	Producer    string `json:"producer"`
}

type Authority struct {
	Kind      AuthorityKind          `json:"kind"`
	Static    *StaticDeclarationPlan `json:"static_declaration_plan,omitempty"`
	Readiness *FlowReadinessPlan     `json:"flow_readiness_plan,omitempty"`
	Ephemeral *EphemeralExecution    `json:"ephemeral_execution,omitempty"`
}

type Admission struct {
	Authority Authority         `json:"authority"`
	Lifetime  ExecutionLifetime `json:"execution_lifetime"`
}

func (a Admission) Equal(other Admission) bool {
	if a.Lifetime != other.Lifetime || a.Authority.Kind != other.Authority.Kind {
		return false
	}
	switch a.Authority.Kind {
	case AuthorityStaticDeclarationPlan:
		return a.Authority.Static != nil && other.Authority.Static != nil &&
			*a.Authority.Static == *other.Authority.Static
	case AuthorityFlowReadinessPlan:
		return a.Authority.Readiness != nil && other.Authority.Readiness != nil &&
			*a.Authority.Readiness == *other.Authority.Readiness
	case AuthorityEphemeralExecution:
		return a.Authority.Ephemeral != nil && other.Authority.Ephemeral != nil &&
			*a.Authority.Ephemeral == *other.Authority.Ephemeral
	default:
		return false
	}
}

func StaticAdmission(sourceSetRevision, bundleHash, bundleSource string, lifetime ExecutionLifetime) (Admission, error) {
	admission := Admission{
		Authority: Authority{
			Kind: AuthorityStaticDeclarationPlan,
			Static: &StaticDeclarationPlan{
				SourceSetRevision: strings.TrimSpace(sourceSetRevision),
				BundleHash:        strings.TrimSpace(bundleHash),
				BundleSource:      strings.TrimSpace(bundleSource),
			},
		},
		Lifetime: lifetime,
	}
	return admission, admission.Validate()
}

func FlowReadinessAdmission(runID, instancePath, planFingerprint string) (Admission, error) {
	admission := Admission{
		Authority: Authority{
			Kind: AuthorityFlowReadinessPlan,
			Readiness: &FlowReadinessPlan{
				RunID:           strings.TrimSpace(runID),
				InstancePath:    strings.Trim(strings.TrimSpace(instancePath), "/"),
				PlanFingerprint: strings.TrimSpace(planFingerprint),
			},
		},
		Lifetime: LifetimeDurableManaged,
	}
	return admission, admission.Validate()
}

func NewEphemeralAdmission(executionID, producer string) (Admission, error) {
	admission := Admission{
		Authority: Authority{
			Kind: AuthorityEphemeralExecution,
			Ephemeral: &EphemeralExecution{
				ExecutionID: strings.TrimSpace(executionID),
				Producer:    strings.TrimSpace(producer),
			},
		},
		Lifetime: LifetimeEphemeral,
	}
	return admission, admission.Validate()
}

func (a Admission) Validate() error {
	switch a.Lifetime {
	case LifetimeDurableManaged, LifetimeEphemeral:
	default:
		return fmt.Errorf("agent topology execution lifetime %q is invalid", a.Lifetime)
	}
	if err := a.Authority.Validate(); err != nil {
		return err
	}
	if a.Authority.Kind == AuthorityEphemeralExecution && a.Lifetime != LifetimeEphemeral {
		return errors.New("ephemeral topology provenance requires ephemeral execution lifetime")
	}
	return nil
}

func (a Authority) Validate() error {
	variants := 0
	if a.Static != nil {
		variants++
	}
	if a.Readiness != nil {
		variants++
	}
	if a.Ephemeral != nil {
		variants++
	}
	if variants != 1 {
		return errors.New("agent topology authority must contain exactly one sealed variant")
	}
	switch a.Kind {
	case AuthorityStaticDeclarationPlan:
		if a.Static == nil || a.Readiness != nil || a.Ephemeral != nil {
			return errors.New("static declaration topology authority has an invalid variant shape")
		}
		if strings.TrimSpace(a.Static.SourceSetRevision) == "" {
			return errors.New("static declaration topology authority requires source_set_revision")
		}
		if err := validateSourceCoordinate(a.Static.BundleHash, a.Static.BundleSource); err != nil {
			return err
		}
	case AuthorityFlowReadinessPlan:
		if a.Readiness == nil || a.Static != nil || a.Ephemeral != nil {
			return errors.New("flow readiness topology authority has an invalid variant shape")
		}
		if _, err := uuid.Parse(strings.TrimSpace(a.Readiness.RunID)); err != nil {
			return fmt.Errorf("flow readiness topology run_id is invalid: %w", err)
		}
		if strings.Trim(strings.TrimSpace(a.Readiness.InstancePath), "/") == "" || strings.TrimSpace(a.Readiness.PlanFingerprint) == "" {
			return errors.New("flow readiness topology authority is incomplete")
		}
	case AuthorityEphemeralExecution:
		if a.Ephemeral == nil || a.Static != nil || a.Readiness != nil {
			return errors.New("ephemeral execution topology authority has an invalid variant shape")
		}
		if _, err := uuid.Parse(strings.TrimSpace(a.Ephemeral.ExecutionID)); err != nil {
			return fmt.Errorf("ephemeral topology execution_id is invalid: %w", err)
		}
		switch strings.TrimSpace(a.Ephemeral.Producer) {
		case "runtime_shard":
		default:
			return fmt.Errorf("ephemeral topology producer %q is invalid", a.Ephemeral.Producer)
		}
	default:
		return fmt.Errorf("agent topology authority kind %q is invalid", a.Kind)
	}
	return nil
}

type SourceCoordinate struct {
	BundleHash   string `json:"bundle_hash"`
	BundleSource string `json:"bundle_source"`
}

func (c SourceCoordinate) Normalize() SourceCoordinate {
	c.BundleHash = strings.TrimSpace(c.BundleHash)
	c.BundleSource = strings.TrimSpace(c.BundleSource)
	return c
}

func (c SourceCoordinate) Validate() error {
	c = c.Normalize()
	return validateSourceCoordinate(c.BundleHash, c.BundleSource)
}

func (c SourceCoordinate) Key() string {
	c = c.Normalize()
	return c.BundleHash + "\x00" + c.BundleSource
}

type DesiredAgent struct {
	Identity       runtimeagentidentity.Plan `json:"identity"`
	Source         SourceCoordinate          `json:"source"`
	ConfigRevision string                    `json:"config_revision"`
}

func (a DesiredAgent) Validate() error {
	if err := a.Identity.Normalize().Validate(); err != nil {
		return fmt.Errorf("desired agent identity: %w", err)
	}
	if err := a.Source.Validate(); err != nil {
		return fmt.Errorf("desired agent source: %w", err)
	}
	if strings.TrimSpace(a.ConfigRevision) == "" {
		return errors.New("desired agent config_revision is required")
	}
	return nil
}

func (a DesiredAgent) Key() (string, error) {
	fingerprint, err := a.Identity.Normalize().Fingerprint()
	if err != nil {
		return "", err
	}
	return fingerprint, nil
}

type SourceSetPlan struct {
	Revision string             `json:"revision"`
	Sources  []SourceCoordinate `json:"sources"`
	Agents   []DesiredAgent     `json:"agents"`
}

type sourceSetPlanContent struct {
	Sources []SourceCoordinate `json:"sources"`
	Agents  []DesiredAgent     `json:"agents"`
}

func NewSourceSetPlan(sources []SourceCoordinate, agents []DesiredAgent) (SourceSetPlan, error) {
	plan := SourceSetPlan{
		Sources: append([]SourceCoordinate(nil), sources...),
		Agents:  append([]DesiredAgent(nil), agents...),
	}
	if err := plan.normalizeAndValidate(false); err != nil {
		return SourceSetPlan{}, err
	}
	revision, err := canonicaljson.Hash(sourceSetPlanContent{Sources: plan.Sources, Agents: plan.Agents})
	if err != nil {
		return SourceSetPlan{}, fmt.Errorf("hash complete agent source set: %w", err)
	}
	plan.Revision = revision
	return plan, plan.Validate()
}

func EmptySourceSetPlan() (SourceSetPlan, error) {
	return NewSourceSetPlan(nil, nil)
}

func (p SourceSetPlan) Validate() error {
	return p.normalizeAndValidate(true)
}

func (p *SourceSetPlan) normalizeAndValidate(requireRevision bool) error {
	if p == nil {
		return errors.New("agent topology source-set plan is required")
	}
	for i := range p.Sources {
		p.Sources[i] = p.Sources[i].Normalize()
		if err := p.Sources[i].Validate(); err != nil {
			return fmt.Errorf("source set source %d: %w", i, err)
		}
	}
	sort.Slice(p.Sources, func(i, j int) bool { return p.Sources[i].Key() < p.Sources[j].Key() })
	for i := 1; i < len(p.Sources); i++ {
		if p.Sources[i-1].Key() == p.Sources[i].Key() {
			return fmt.Errorf("source set repeats source %q", p.Sources[i].Key())
		}
	}
	for i := range p.Agents {
		p.Agents[i].Identity = p.Agents[i].Identity.Normalize()
		p.Agents[i].Source = p.Agents[i].Source.Normalize()
		p.Agents[i].ConfigRevision = strings.TrimSpace(p.Agents[i].ConfigRevision)
		if err := p.Agents[i].Validate(); err != nil {
			return fmt.Errorf("source set agent %d: %w", i, err)
		}
	}
	var keyErr error
	sort.Slice(p.Agents, func(i, j int) bool {
		left, err := p.Agents[i].Key()
		if err != nil {
			keyErr = err
			return false
		}
		right, err := p.Agents[j].Key()
		if err != nil {
			keyErr = err
			return false
		}
		return left < right
	})
	if keyErr != nil {
		return keyErr
	}
	for i := 1; i < len(p.Agents); i++ {
		left, _ := p.Agents[i-1].Key()
		right, _ := p.Agents[i].Key()
		if left == right {
			return fmt.Errorf("source set repeats agent declaration plan %q", p.Agents[i].Identity.Description())
		}
	}
	if requireRevision {
		p.Revision = strings.TrimSpace(p.Revision)
		if p.Revision == "" {
			return errors.New("agent topology source-set revision is required")
		}
		want, err := canonicaljson.Hash(sourceSetPlanContent{Sources: p.Sources, Agents: p.Agents})
		if err != nil {
			return err
		}
		if p.Revision != want {
			return errors.New("agent topology source-set revision does not match canonical plan")
		}
	}
	return nil
}

func validateSourceCoordinate(bundleHash, bundleSource string) error {
	if err := runtimebundleidentity.ValidateCanonicalHash(strings.TrimSpace(bundleHash)); err != nil {
		return fmt.Errorf("agent topology bundle_hash is invalid: %w", err)
	}
	switch strings.TrimSpace(bundleSource) {
	case "persisted", "ephemeral":
		return nil
	default:
		return fmt.Errorf("agent topology bundle_source %q is invalid", bundleSource)
	}
}

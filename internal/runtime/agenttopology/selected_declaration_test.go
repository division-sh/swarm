package agenttopology

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/google/uuid"
)

func TestSelectedDeclarationPlanRejectsUnboundAndCorruptAuthority(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	plan, err := NewSelectedDeclarationPlan(hash, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Agents == nil || len(plan.Agents) != 0 {
		t.Fatal("empty declaration census is not explicit")
	}
	for _, tc := range []struct {
		name string
		edit func(*SelectedDeclarationPlan)
	}{
		{"missing census", func(p *SelectedDeclarationPlan) { p.Agents = nil }},
		{"changed fingerprint", func(p *SelectedDeclarationPlan) { p.Revision = "sha256:" + strings.Repeat("b", 64) }},
		{"changed target", func(p *SelectedDeclarationPlan) { p.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64) }},
		{"unadmitted agent", func(p *SelectedDeclarationPlan) { p.Agents = append(p.Agents, DesiredAgent{}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := plan
			tc.edit(&bad)
			if err := bad.Validate(); err == nil {
				t.Fatal("accepted corrupt declaration plan")
			}
		})
	}
	admission, err := SelectedDeclarationAdmission(uuid.NewString(), plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*Admission)
	}{
		{"live source set", func(a *Admission) {
			a.Authority.Static = &StaticDeclarationPlan{SourceSetRevision: "live", BundleHash: hash}
		}},
		{"readiness", func(a *Admission) { a.Authority.Readiness = &FlowReadinessPlan{} }},
		{"ephemeral", func(a *Admission) { a.Lifetime = LifetimeEphemeral }},
		{"wrong kind", func(a *Admission) { a.Authority.Kind = AuthorityStaticDeclarationPlan }},
		{"missing run", func(a *Admission) { a.Authority.Selected.RunID = "" }},
		{"zero run", func(a *Admission) { a.Authority.Selected.RunID = uuid.Nil.String() }},
		{"malformed fingerprint", func(a *Admission) { a.Authority.Selected.PlanFingerprint = "fingerprint" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := admission
			selected := *admission.Authority.Selected
			bad.Authority.Selected = &selected
			tc.edit(&bad)
			if err := bad.Validate(); err == nil {
				t.Fatal("accepted malformed selected authority")
			}
		})
	}
	if !admission.Equal(admission) || admission.Equal(Admission{}) {
		t.Fatal("selected declaration equality is not exact")
	}
}

func TestSelectedDeclarationPlanCanonicalCensus(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	makeAgent := func(id string) DesiredAgent {
		t.Helper()
		name, err := agentidentity.DeclaredName(id, ".")
		if err != nil {
			t.Fatal(err)
		}
		identity, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
		if err != nil {
			t.Fatal(err)
		}
		return DesiredAgent{Identity: identity, Source: SourceCoordinate{BundleHash: hash}, ConfigRevision: strings.Repeat("c", 64)}
	}
	a, b := makeAgent("a"), makeAgent("b")
	input := []DesiredAgent{b, a}
	plan, err := NewSelectedDeclarationPlan(hash, input)
	if err != nil {
		t.Fatal(err)
	}
	if input[0] != b || plan.Agents[0] != a {
		t.Fatal("constructor mutated input or did not canonicalize order")
	}
	input[0].ConfigRevision = "changed"
	if err := plan.Validate(); err != nil {
		t.Fatalf("plan retained caller's mutable slice: %v", err)
	}
	if _, err := NewSelectedDeclarationPlan(hash, []DesiredAgent{a, a}); err == nil {
		t.Fatal("accepted duplicate declarations")
	}
	plan.Agents[0], plan.Agents[1] = plan.Agents[1], plan.Agents[0]
	if err := plan.Validate(); err == nil {
		t.Fatal("read-time validation repaired noncanonical order")
	}
}

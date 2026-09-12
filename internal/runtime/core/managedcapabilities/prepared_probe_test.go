package managedcapabilities

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/google/uuid"
)

func preparedProbePlan(t *testing.T) Plan {
	t.Helper()
	actor := managedCapabilityTestPlan(t, "worker")
	fingerprint, err := actor.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	return Plan{
		ActorPlan: actor, RuntimeMode: "startup_probe", Provider: "test", Transport: "cli",
		ProviderContract: "test.v1", CreatedAt: time.Unix(1, 0).UTC(),
		Authority: Authority{
			Kind: AuthorityStartupProbe, ID: uuid.NewString(),
			ExecutionKind: ExecutionSelectedForkPreparation, ExecutionAuthorityID: uuid.NewString(),
			Preparation: &PreparedSelectedForkProbeAuthority{
				SelectedForkPreparationCoordinates: SelectedForkPreparationCoordinates{
					ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "process-owner", ProcessBootID: uuid.NewString(),
					BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), SourceFingerprint: strings.Repeat("b", 64),
					AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64),
					CatalogFingerprint: strings.Repeat("e", 64),
				},
				ActorPlanFingerprint: fingerprint,
			},
		},
	}
}

func TestPreparedSelectedForkProbeAuthorityClosedVariants(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Plan)
	}{
		{"missing_evidence", func(p *Plan) { p.Authority.Preparation = nil }},
		{"missing_preparation", func(p *Plan) { p.Authority.ExecutionAuthorityID = "" }},
		{"zero_preparation", func(p *Plan) { p.Authority.ExecutionAuthorityID = uuid.Nil.String() }},
		{"provider_turn", func(p *Plan) {
			p.Authority.Kind = AuthorityProviderTurn
			p.Authority.SessionID, p.Authority.TurnOrdinal = uuid.NewString(), 1
		}},
		{"normal_startup", func(p *Plan) {
			p.Authority.ExecutionKind = ExecutionNormalAgent
			p.Authority.StartupOwnerID, p.Authority.StartupGeneration = "startup", 1
		}},
		{"bound_selected", func(p *Plan) {
			p.Authority.ExecutionKind = ExecutionSelectedContractFork
			p.Authority.StartupOwnerID, p.Authority.StartupGeneration = "startup", 1
			p.Authority.RunID = uuid.NewString()
		}},
		{"execution_run", func(p *Plan) { p.Authority.RunID = uuid.NewString() }},
		{"session", func(p *Plan) { p.Authority.SessionID = uuid.NewString() }},
		{"turn", func(p *Plan) { p.Authority.TurnOrdinal = 1 }},
		{"startup_owner", func(p *Plan) { p.Authority.StartupOwnerID = "startup" }},
		{"startup_generation", func(p *Plan) { p.Authority.StartupGeneration = 1 }},
		{"live_actor", func(p *Plan) {
			p.ActorIdentity = managedCapabilityTestIdentity("worker")
			p.ActorPlan = agentidentity.Plan{}
		}},
		{"dual_actor", func(p *Plan) { p.ActorIdentity = managedCapabilityTestIdentity("worker") }},
		{"wrong_actor_plan", func(p *Plan) { p.ActorPlan = managedCapabilityTestPlan(t, "other") }},
		{"same_name_wrong_owner", func(p *Plan) { p.ActorPlan.Name.Owner = "other-owner" }},
		{"same_name_wrong_route", func(p *Plan) {
			p.ActorPlan.Route = managedCapabilityTestRoutedIdentity("worker", "other-instance").Route
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := preparedProbePlan(t)
			test.mutate(&plan)
			if _, err := New(plan); err == nil {
				t.Fatal("accepted malformed or executable preparation")
			}
		})
	}
}

func TestPreparedSelectedForkProbeEvidenceIdentity(t *testing.T) {
	base := preparedProbePlan(t)
	original, err := New(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := original.Validate(); err != nil {
		t.Fatal(err)
	}
	if original.MatchesActor(managedCapabilityTestIdentity("worker")) || !original.MatchesActorPlan(base.ActorPlan) {
		t.Fatal("preparation did not preserve its non-live actor owner")
	}
	for _, test := range []struct {
		name        string
		field       func(*PreparedSelectedForkProbeAuthority) *string
		replacement string
	}{
		{"process", func(p *PreparedSelectedForkProbeAuthority) *string { return &p.ProcessAuthorityID }, uuid.NewString()},
		{"boot", func(p *PreparedSelectedForkProbeAuthority) *string { return &p.ProcessBootID }, uuid.NewString()},
		{"owner", func(p *PreparedSelectedForkProbeAuthority) *string { return &p.ProcessOwnerID }, "other-process"},
		{"bundle", func(p *PreparedSelectedForkProbeAuthority) *string { return &p.BundleHash }, "bundle-v2:sha256:" + strings.Repeat("1", 64)},
		{"source", func(p *PreparedSelectedForkProbeAuthority) *string { return &p.SourceFingerprint }, strings.Repeat("2", 64)},
		{"plan", func(p *PreparedSelectedForkProbeAuthority) *string { return &p.AdmittedPlanFingerprint }, strings.Repeat("3", 64)},
		{"config", func(p *PreparedSelectedForkProbeAuthority) *string { return &p.ConfigurationFingerprint }, strings.Repeat("4", 64)},
		{"catalog", func(p *PreparedSelectedForkProbeAuthority) *string { return &p.CatalogFingerprint }, strings.Repeat("5", 64)},
		{"actor", func(p *PreparedSelectedForkProbeAuthority) *string { return &p.ActorPlanFingerprint }, strings.Repeat("6", 64)},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := base
			plan.Authority = base.Authority.clone()
			*test.field(plan.Authority.Preparation) = ""
			if _, err := New(plan); err == nil {
				t.Fatal("accepted missing preparation coordinate")
			}
			*test.field(plan.Authority.Preparation) = test.replacement
			if test.name == "actor" {
				if _, err := New(plan); err == nil {
					t.Fatal("accepted crossed prospective actor receipt")
				}
				return
			}
			changed, err := New(plan)
			if err != nil {
				t.Fatal(err)
			}
			before, err := original.PlanFingerprint()
			if err != nil {
				t.Fatal(err)
			}
			after, err := changed.PlanFingerprint()
			if err != nil {
				t.Fatal(err)
			}
			if original.ID == changed.ID || before == after {
				t.Fatal("changed preparation coordinate did not change exact probe identity")
			}
			mutated := original.Clone()
			*test.field(mutated.Authority.Preparation) = test.replacement
			if err := mutated.Validate(); err == nil {
				t.Fatal("accepted altered receipt with original integrity identity")
			}
		})
	}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Surface
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal(err)
	}
	base.Authority.Preparation.CatalogFingerprint = strings.Repeat("f", 64)
	copy := original.Clone()
	copy.Authority.Preparation.ConfigurationFingerprint = strings.Repeat("f", 64)
	if err := original.Validate(); err != nil {
		t.Fatalf("caller or clone mutation changed original preparation: %v", err)
	}
}

func TestPreparedSelectedForkProbeRejectsNoncanonicalEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*PreparedSelectedForkProbeAuthority)
	}{
		{"unprefixed_bundle", func(p *PreparedSelectedForkProbeAuthority) { p.BundleHash = strings.Repeat("a", 64) }},
		{"uppercase_bundle", func(p *PreparedSelectedForkProbeAuthority) {
			p.BundleHash = "bundle-v2:sha256:" + strings.Repeat("A", 64)
		}},
		{"padded_bundle", func(p *PreparedSelectedForkProbeAuthority) { p.BundleHash += " " }},
		{"zero_process", func(p *PreparedSelectedForkProbeAuthority) { p.ProcessAuthorityID = uuid.Nil.String() }},
		{"malformed_process", func(p *PreparedSelectedForkProbeAuthority) { p.ProcessAuthorityID = "process" }},
		{"padded_owner", func(p *PreparedSelectedForkProbeAuthority) { p.ProcessOwnerID += " " }},
		{"zero_boot", func(p *PreparedSelectedForkProbeAuthority) { p.ProcessBootID = uuid.Nil.String() }},
		{"uppercase_source", func(p *PreparedSelectedForkProbeAuthority) { p.SourceFingerprint = strings.Repeat("B", 64) }},
		{"nonhex_plan", func(p *PreparedSelectedForkProbeAuthority) { p.AdmittedPlanFingerprint = strings.Repeat("z", 64) }},
		{"short_config", func(p *PreparedSelectedForkProbeAuthority) { p.ConfigurationFingerprint = strings.Repeat("d", 63) }},
		{"padded_catalog", func(p *PreparedSelectedForkProbeAuthority) { p.CatalogFingerprint += " " }},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := preparedProbePlan(t)
			test.mutate(plan.Authority.Preparation)
			if _, err := New(plan); err == nil {
				t.Fatal("accepted noncanonical preparation evidence")
			}
		})
	}
}

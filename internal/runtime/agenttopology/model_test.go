package agenttopology

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestAdmissionEqualComparesTypedFactsAcrossReadback(t *testing.T) {
	original, err := StaticAdmission(
		strings.Repeat("a", 64),
		"bundle-v1:sha256:"+strings.Repeat("b", 64),
		"persisted",
		LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Admission
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !original.Equal(decoded) || !decoded.Equal(original) {
		t.Fatal("typed admission facts changed across JSON readback")
	}

	different, err := StaticAdmission(
		strings.Repeat("c", 64),
		"bundle-v1:sha256:"+strings.Repeat("b", 64),
		"persisted",
		LifetimeDurableManaged,
	)
	if err != nil {
		t.Fatal(err)
	}
	if original.Equal(different) {
		t.Fatal("different source-set revisions compared equal")
	}
}

func TestAdmissionSealedAuthorityAndLifetimeMatrix(t *testing.T) {
	const (
		revision   = "source-set-v1"
		bundleHash = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	runID := uuid.NewString()
	executionID := uuid.NewString()

	staticDurable, err := StaticAdmission(revision, bundleHash, "persisted", LifetimeDurableManaged)
	if err != nil {
		t.Fatalf("static durable admission: %v", err)
	}
	staticEphemeral, err := StaticAdmission(revision, bundleHash, "persisted", LifetimeEphemeral)
	if err != nil {
		t.Fatalf("static ephemeral admission: %v", err)
	}
	flowDurable, err := FlowReadinessAdmission(runID, "flow/instance", "plan-v1")
	if err != nil {
		t.Fatalf("flow readiness admission: %v", err)
	}
	ephemeral, err := NewEphemeralAdmission(executionID, "runtime_shard")
	if err != nil {
		t.Fatalf("ephemeral execution admission: %v", err)
	}

	for name, admission := range map[string]Admission{
		"static_durable":   staticDurable,
		"static_ephemeral": staticEphemeral,
		"flow_durable":     flowDurable,
		"ephemeral":        ephemeral,
	} {
		t.Run(name, func(t *testing.T) {
			if err := admission.Validate(); err != nil {
				t.Fatalf("valid admission rejected: %v", err)
			}
		})
	}

	tests := map[string]Admission{
		"zero_variant": {
			Authority: Authority{Kind: AuthorityStaticDeclarationPlan},
			Lifetime:  LifetimeDurableManaged,
		},
		"multiple_variants": {
			Authority: Authority{
				Kind:      AuthorityStaticDeclarationPlan,
				Static:    staticDurable.Authority.Static,
				Readiness: flowDurable.Authority.Readiness,
			},
			Lifetime: LifetimeDurableManaged,
		},
		"ephemeral_authority_with_durable_lifetime": {
			Authority: ephemeral.Authority,
			Lifetime:  LifetimeDurableManaged,
		},
		"kind_variant_mismatch": {
			Authority: Authority{Kind: AuthorityFlowReadinessPlan, Static: staticDurable.Authority.Static},
			Lifetime:  LifetimeDurableManaged,
		},
	}
	for name, admission := range tests {
		t.Run(name, func(t *testing.T) {
			if err := admission.Validate(); err == nil {
				t.Fatal("invalid admission was accepted")
			}
		})
	}
}

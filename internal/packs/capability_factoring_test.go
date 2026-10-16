package packs

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func assertFactoringSubjectError(t *testing.T, err error, want string) {
	t.Helper()
	got := ""
	if err != nil {
		got = err.Error()
	}
	if got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestFactoringCapabilitySubject(t *testing.T) {
	base := Subject{ID: "tool", Kind: SubjectProviderConnector, Provider: "acme", Source: "flow_local", Applicability: "effective"}
	for _, tc := range []struct {
		name   string
		change func(*Subject)
		want   string
	}{
		{"identity-order", func(s *Subject) { s.ID = " "; s.Kind = "unknown" }, "capability subject id, provider, source, and applicability are required"},
		{"requirement/shape-before-kind", func(s *Subject) { s.Kind = "unknown"; s.Requirements = []Requirement{{Name: "x"}} }, `capability subject "tool" requirement kind, name, and scope are required`},
		{"identity-order/kind", func(s *Subject) { s.Kind = "unknown" }, `capability subject "tool" has unsupported kind "unknown"`},
		{"connector/installed-source", func(s *Subject) { s.Applicability = "installed" }, `installed provider connector subject "tool" must use connector_pack source`},
		{"connector/effective-source", func(s *Subject) { s.Source = "connector_pack" }, `effective provider connector subject "tool" has invalid source "connector_pack"`},
		{"connector/applicability", func(s *Subject) { s.Applicability = "unknown" }, `provider connector subject "tool" has invalid applicability "unknown"`},
		{"connector/missing-import", func(s *Subject) { s.Applicability, s.Source = "installed", "connector_pack" }, `installed provider connector subject "tool" must carry an import requirement`},
		{"connector/forbidden-import", func(s *Subject) {
			s.Requirements = []Requirement{RequirementWithStatus(RequirementImport, "tool", "package", RequirementStatusNotImported, "registry")}
		}, `effective provider connector subject "tool" must not carry import requirement "tool"`},
		{"requirement/unevaluated", func(s *Subject) { s.Requirements = []Requirement{TargetScopedRequirement(RequirementSecret, "key")} }, `provider connector subject "tool" requirement "key" must be evaluated`},
		{"requirement/status", func(s *Subject) {
			s.Requirements = []Requirement{RequirementWithStatus(RequirementSecret, "key", "deployment", "INVALID", "file")}
		}, `provider connector subject "tool" requirement "key" has invalid secret status "INVALID"`},
		{"requirement/satisfaction", func(s *Subject) {
			r := RequirementWithStatus(RequirementSecret, "key", "deployment", RequirementStatusUnbound, "file")
			r.Satisfied = boolPointer(true)
			s.Requirements = []Requirement{r}
		}, `provider connector subject "tool" requirement "key" status "UNBOUND" contradicts satisfied=true`},
		{"requirement/remediation", func(s *Subject) {
			r := RequirementWithStatus(RequirementSecret, "key", "deployment", RequirementStatusUnbound, "file")
			r.Remediation = "other"
			s.Requirements = []Requirement{r}
		}, `provider connector subject "tool" requirement "key" remediation does not match canonical secret/UNBOUND remediation`},
		{"channel-plan/source", func(s *Subject) { s.Kind = SubjectChannelPack }, `channel pack subject "tool" must use channel_pack/installed authority`},
		{"channel-plan/requirements", func(s *Subject) {
			s.Kind, s.Source, s.Applicability = SubjectChannelPack, "channel_pack", "installed"
			s.Requirements = []Requirement{TargetScopedRequirement(RequirementSecret, "key")}
		}, `channel pack subject "tool" must not carry runtime requirements`},
		{"channel-binding/source", func(s *Subject) { s.Kind, s.Source = SubjectChannelOutbound, "channel_outbound_binding" }, `channel outbound subject "tool" must use channel_binding/effective authority`},
		{"channel-binding/import", func(s *Subject) {
			s.Kind, s.Source = SubjectChannelOutbound, "channel_binding"
			s.Requirements = []Requirement{RequirementWithStatus(RequirementImport, "tool", "package", RequirementStatusNotImported, "registry")}
		}, `channel outbound subject "tool" must not carry package import requirements`},
		{"status-order", func(s *Subject) { s.Status = StatusNotReady; s.Guarantees = []Guarantee{{Code: "invalid"}} }, `capability subject "tool" status "NOT_READY" contradicts derived status "READY"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subject := CloneSubjects([]Subject{base})[0]
			tc.change(&subject)
			before, _ := json.Marshal(subject)
			_, err := NormalizeSubjects([]Subject{subject})
			assertFactoringSubjectError(t, err, tc.want)
			after, _ := json.Marshal(subject)
			if string(before) != string(after) {
				t.Fatal("failed normalization mutated caller")
			}
		})
	}
	t.Run("requirement/finite-status-matrix", func(t *testing.T) {
		for _, tc := range []struct {
			kind, status string
			satisfied    bool
		}{
			{RequirementSecret, RequirementStatusBound, true}, {RequirementSecret, RequirementStatusUnbound, false},
			{RequirementManagedCredential, RequirementStatusConnected, true}, {RequirementManagedCredential, RequirementStatusUnconnected, false},
			{RequirementManagedCredential, RequirementStatusPendingConsent, false}, {RequirementManagedCredential, RequirementStatusRefreshFailed, false},
			{RequirementManagedCredential, RequirementStatusScopeInsufficient, false},
		} {
			t.Run(tc.kind+"/"+tc.status, func(t *testing.T) {
				s := base
				s.Requirements = []Requirement{RequirementWithStatus(tc.kind, "key", "deployment", tc.status, "owner")}
				got, err := NormalizeSubjects([]Subject{s})
				assertFactoringSubjectError(t, err, "")
				want := StatusNotReady
				if tc.satisfied {
					want = StatusReady
				}
				if got[0].Status != want {
					t.Fatalf("status = %s, want %s", got[0].Status, want)
				}
			})
		}
	})
	t.Run("sort-dedup-and-facts", func(t *testing.T) {
		s := base
		s.ID, s.Provider, s.Action, s.Source, s.Provenance, s.SourcePath, s.Applicability = " tool ", "acme", " write ", " flow_local ", " external ", " tools.yaml ", " effective "
		s.Capabilities = []Capability{{Code: " z_unknown ", Target: " z "}, {Code: " a_unknown ", Target: " a "}}
		s.Requirements = []Requirement{RequirementWithStatus(RequirementSecret, "z", "deployment", RequirementStatusBound, "file"), RequirementWithStatus(RequirementSecret, "a", "deployment", RequirementStatusUnbound, "file")}
		s.Guarantees = []Guarantee{{Code: GuaranteeActivityJournal}}
		got, err := NormalizeSubjects([]Subject{s})
		assertFactoringSubjectError(t, err, "")
		if got[0].ID != "tool" || got[0].Status != StatusNotReady || got[0].Capabilities[0] != (Capability{Code: "a_unknown", Target: "a"}) || got[0].Requirements[0].Name != "a" {
			t.Fatalf("normalized = %#v", got)
		}
		again, err := NormalizeSubjects(got)
		if err != nil || !reflect.DeepEqual(got, again) {
			t.Fatalf("not idempotent: %v", err)
		}
		_, err = NormalizeSubjects(append(got, got[0]))
		assertFactoringSubjectError(t, err, "duplicate capability subject provider_connector/tool")
		want := "provider connector tool NOT READY; provenance=external; source_path=tools.yaml; requires a=UNBOUND (fix: swarm secrets set a); requires z=BOUND; CAN a_unknown a; CAN z_unknown z; guarantee: cannot bypass activity_attempts - enforced by internal/runtime/pipeline.pipelineActivityDispatcher.executeNonIdempotentActivityIntent"
		if rendered := RenderSubject(got[0], false); rendered != want {
			t.Fatalf("text = %q, want %q", rendered, want)
		}
	})
}

func factoringTriggerSubject(t *testing.T) Subject {
	t.Helper()
	id, err := IngressSubjectID("bundle", "flow", "acme")
	if err != nil {
		t.Fatal(err)
	}
	return Subject{ID: id, Kind: SubjectProviderTrigger, Provider: "acme", Source: "raw_declaration", Applicability: "effective",
		TriggerAdmission: &TriggerAdmission{BundleHash: "bundle", FlowPath: "flow", Alias: "alias", CatalogGeneration: "generation", PolicySource: "raw_declaration", RequestAuthentication: "HMAC_SHA256", Event: "inbound.acme"},
		Requirements:     []Requirement{TargetScopedRequirement(RequirementSecret, "key")}}
}

func TestFactoringTriggerSubject(t *testing.T) {
	base := factoringTriggerSubject(t)
	for _, tc := range []struct {
		name   string
		change func(*Subject)
		want   string
		status SubjectStatus
	}{
		{"descriptors/shape-first", func(s *Subject) {
			s.TriggerEvents = []TriggerEventDescriptor{{Event: "bad", Kind: "other"}}
			s.TriggerAdmission = nil
		}, `provider trigger subject %q has invalid trigger event descriptor`, ""},
		{"descriptors/field", func(s *Subject) {
			s.TriggerEvents = []TriggerEventDescriptor{{Event: "event", Kind: "raw", Fields: []TriggerEventFieldDescriptor{{Name: "x"}}}}
		}, `provider trigger subject %q event "event" has incomplete field descriptor`, ""},
		{"descriptors/duplicate", func(s *Subject) {
			s.TriggerEvents = []TriggerEventDescriptor{{Event: "event", Kind: "raw"}, {Event: "event", Kind: "normalized"}}
		}, `provider trigger subject %q has duplicate event descriptor "event"`, ""},
		{"installed/admission", func(s *Subject) { s.Source, s.Applicability = "trigger_pack", "installed" }, `installed provider trigger subject %q must use trigger_pack source and must not carry target admission`, ""},
		{"coordinate/source", func(s *Subject) { s.Source = "other" }, `effective provider trigger subject %q has invalid source "other"`, ""},
		{"coordinate/admission", func(s *Subject) { s.TriggerAdmission = nil }, `effective provider trigger subject %q requires typed trigger_admission`, ""},
		{"coordinate/bundle", func(s *Subject) { s.TriggerAdmission.BundleHash = " " }, `effective provider trigger subject %q requires bundle_hash, alias, catalog_generation, and event`, ""},
		{"coordinate/catalog", func(s *Subject) { s.TriggerAdmission.CatalogGeneration = " " }, `effective provider trigger subject %q requires bundle_hash, alias, catalog_generation, and event`, ""},
		{"coordinate/event", func(s *Subject) { s.TriggerAdmission.Event = " " }, `effective provider trigger subject %q requires bundle_hash, alias, catalog_generation, and event`, ""},
		{"policy/source", func(s *Subject) { s.TriggerAdmission.PolicySource = "other" }, `effective provider trigger subject %q has invalid policy_source "other"`, ""},
		{"policy/auth", func(s *Subject) { s.TriggerAdmission.RequestAuthentication = "other" }, `effective provider trigger subject %q has invalid request_authentication "other"`, ""},
		{"policy/pack-required", func(s *Subject) { s.TriggerAdmission.PolicySource = "verified_pack" }, `effective verified-pack trigger subject %q requires trigger_pack_binding source and pack identity`, ""},
		{"policy/raw-no-pack", func(s *Subject) { s.TriggerAdmission.Pack = &TriggerPackIdentity{} }, `effective raw trigger subject %q must use raw_declaration source without pack identity`, ""},
		{"auth-count-scope/unsigned-requirement", func(s *Subject) { s.TriggerAdmission.RequestAuthentication = "UNAUTHENTICATED" }, `effective unauthenticated provider trigger subject %q must not carry secret requirements`, ""},
		{"auth-count-scope/missing-secret", func(s *Subject) { s.Requirements = nil }, `effective authenticated provider trigger subject %q must carry exactly one target secret requirement`, ""},
		{"auth-count-scope/wrong-kind", func(s *Subject) { s.Requirements[0].Kind = RequirementImport }, `effective authenticated provider trigger subject %q requirement "key" must be a secret`, ""},
		{"auth-count-scope/wrong-scope", func(s *Subject) { s.Requirements[0].Scope = "deployment" }, `provider trigger subject %q requirement "key" must be target-scoped`, ""},
		{"evaluation/base", func(s *Subject) {}, "", StatusAvailable},
		{"evaluation/bound", func(s *Subject) {
			s.Requirements[0] = RequirementWithStatus(RequirementSecret, "key", RequirementScopeTarget, RequirementStatusBound, "credential_store")
		}, "", StatusReady},
		{"evaluation/unbound", func(s *Subject) {
			s.Requirements[0] = RequirementWithStatus(RequirementSecret, "key", RequirementScopeTarget, RequirementStatusUnbound, "credential_store")
		}, "", StatusNotReady},
		{"evaluation/partial", func(s *Subject) { s.Requirements[0].Status = RequirementStatusBound }, `effective provider trigger subject %q requirement "key" must be wholly evaluated or wholly unevaluated`, ""},
		{"evaluation/wrong-source", func(s *Subject) {
			s.Requirements[0] = RequirementWithStatus(RequirementSecret, "key", RequirementScopeTarget, RequirementStatusBound, "file")
		}, `effective provider trigger subject %q requirement "key" must use credential_store source`, ""},
		{"disabled-authenticated", func(s *Subject) {
			s.TriggerAdmission.BindingEnabled = boolPointer(false)
			s.Requirements[0] = RequirementWithStatus(RequirementSecret, "key", RequirementScopeTarget, RequirementStatusBound, "credential_store")
		}, "", StatusNotReady},
		{"disabled-base", func(s *Subject) { s.TriggerAdmission.BindingEnabled = boolPointer(false) }, "", StatusNotReady},
		{"disabled-unsigned", func(s *Subject) {
			s.TriggerAdmission.BindingEnabled = boolPointer(false)
			s.TriggerAdmission.RequestAuthentication = "UNAUTHENTICATED"
			s.Requirements = nil
		}, "", StatusNotReady},
		{"unsigned", func(s *Subject) { s.TriggerAdmission.RequestAuthentication = "UNAUTHENTICATED"; s.Requirements = nil }, "", StatusReady},
		{"block-reason-order/enabled", func(s *Subject) { s.TriggerAdmission.BindingBlockReason = "invalid" }, `effective provider trigger subject %q has a blocked enabled binding`, ""},
		{"block-reason-order/finite", func(s *Subject) {
			s.TriggerAdmission.BindingEnabled = boolPointer(false)
			s.TriggerAdmission.BindingBlockReason = "invalid"
		}, `effective provider trigger subject %q has invalid binding_block_reason "invalid"`, ""},
		{"block-reason-order/credentials", func(s *Subject) {
			s.TriggerAdmission.BindingEnabled = boolPointer(false)
			s.TriggerAdmission.BindingBlockReason = "credentials_absent"
		}, "", StatusNotReady},
		{"recovery-tuple/missing-operation", func(s *Subject) {
			s.TriggerAdmission.BindingEnabled = boolPointer(false)
			s.TriggerAdmission.BindingBlockReason = "recovery_required"
		}, `effective provider trigger subject %q has contradictory recovery operation evidence`, ""},
		{"recovery-tuple/missing-command", func(s *Subject) {
			s.TriggerAdmission.BindingEnabled = boolPointer(false)
			s.TriggerAdmission.BindingBlockReason = "recovery_required"
			s.TriggerAdmission.RecoveryOperationID = "op"
		}, `effective provider trigger subject %q has contradictory recovery command evidence`, ""},
		{"recovery-tuple/foreign-operation", func(s *Subject) { s.TriggerAdmission.RecoveryOperationID = "op" }, `effective provider trigger subject %q has contradictory recovery operation evidence`, ""},
		{"recovery-tuple/foreign-command", func(s *Subject) { s.TriggerAdmission.RecoveryCommand = "recover" }, `effective provider trigger subject %q has contradictory recovery command evidence`, ""},
		{"recovery-tuple/exact-bytes", func(s *Subject) {
			s.TriggerAdmission.BindingEnabled = boolPointer(false)
			s.TriggerAdmission.BindingBlockReason = "recovery_required"
			s.TriggerAdmission.RecoveryOperationID = " op "
			s.TriggerAdmission.RecoveryCommand = " recover "
		}, "", StatusNotReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := CloneSubjects([]Subject{base})[0]
			tc.change(&s)
			before, _ := json.Marshal(s)
			got, err := NormalizeSubjects([]Subject{s})
			want := tc.want
			if want != "" {
				want = fmt.Sprintf(want, base.ID)
			}
			assertFactoringSubjectError(t, err, want)
			if err == nil && got[0].Status != tc.status {
				t.Fatalf("status = %s, want %s", got[0].Status, tc.status)
			}
			after, _ := json.Marshal(s)
			if string(before) != string(after) {
				t.Fatal("normalization mutated caller's admission")
			}
		})
	}
	t.Run("flow-identity", func(t *testing.T) {
		changed := CloneSubjects([]Subject{base})[0]
		changed.TriggerAdmission.Alias = "different-alias"
		got, err := NormalizeSubjects([]Subject{changed})
		assertFactoringSubjectError(t, err, "")
		if got[0].ID != base.ID {
			t.Fatal("alias changed declaring-flow identity")
		}
		changed.TriggerAdmission.FlowPath = "different-flow"
		_, err = NormalizeSubjects([]Subject{changed})
		wantID, _ := IngressSubjectID("bundle", "different-flow", "acme")
		assertFactoringSubjectError(t, err, fmt.Sprintf("effective provider trigger subject %q must use stable target id %q", base.ID, wantID))
	})
	t.Run("disabled-admission-clone", func(t *testing.T) {
		s := base
		s.TriggerAdmission.BindingEnabled = boolPointer(false)
		s.Evidence = []Evidence{{Kind: "test", Fields: map[string]string{"generation": "old"}}}
		got, err := NormalizeSubjects([]Subject{s})
		assertFactoringSubjectError(t, err, "")
		*got[0].TriggerAdmission.BindingEnabled = true
		got[0].Evidence[0].Fields["generation"] = "changed"
		if *s.TriggerAdmission.BindingEnabled || s.Evidence[0].Fields["generation"] != "old" {
			t.Fatal("admission readback aliases caller state")
		}
	})
	t.Run("strict-alias", func(t *testing.T) {
		for _, alias := range []string{" alias", "alias ", "/alias", "alias/"} {
			s := CloneSubjects([]Subject{base})[0]
			s.TriggerAdmission.Alias = alias
			_, err := NormalizeSubjects([]Subject{s})
			if err == nil || !strings.Contains(err.Error(), "alias") {
				t.Fatalf("noncanonical alias %q: %v", alias, err)
			}
		}
	})
}

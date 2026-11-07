package channelnative

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func TestNativeInboxTransportParentRequiresExactDisjointEvidence(t *testing.T) {
	plan, err := plangeneration.FromCanonicalValue(map[string]string{"channel": "deployment"})
	if err != nil {
		t.Fatal(err)
	}
	contract, err := EntryContractHash(plan)
	if err != nil {
		t.Fatal(err)
	}
	legacy := Admission{Provider: "telegram", ResourceSlotID: "telegram:synthetic",
		ConversationReference: "operator", PrincipalID: uuid.NewString(), InterfaceKey: "channel",
		BindingRevision: 2, ActivationID: uuid.NewString(), ActivationRevision: 3,
		ContextPublicationGeneration: 4, PackID: "telegram", PackVersion: "1", PackManifestHash: "manifest",
		PlanGeneration: plan, EntryContractHash: contract}
	if err := legacy.Validate(); err != nil {
		t.Fatalf("existing runtime admission rejected: %v", err)
	}
	runtime, err := legacy.TransportParent()
	if err != nil || runtime.Kind != TransportRuntimeActivation || runtime.ID != legacy.ActivationID || runtime.Revision != legacy.ActivationRevision {
		t.Fatalf("existing runtime parent changed: %+v err=%v", runtime, err)
	}
	explicit := legacy
	explicit.Parent = runtime
	if err := explicit.Validate(); err != nil {
		t.Fatal(err)
	}
	deployment := legacy
	deployment.ActivationID, deployment.ActivationRevision, deployment.ContextPublicationGeneration = "", 0, 0
	deployment.Parent = TransportParent{Kind: TransportDeploymentRegistration,
		ID: uuid.NewString(), Revision: 5, BindingOperationID: uuid.NewString(), BindingOperationRevision: 3,
		ProofID: uuid.NewString(), ProofRevision: 2}
	if err := deployment.Validate(); err != nil {
		t.Fatalf("explicit deployment provenance rejected: %v", err)
	}
	for name, edit := range map[string]func(*Admission){
		"missing_tag":                    func(a *Admission) { a.Parent.Kind = "" },
		"missing_parent":                 func(a *Admission) { a.Parent = TransportParent{} },
		"missing_proof":                  func(a *Admission) { a.Parent.ProofID = "" },
		"mixed_activation":               func(a *Admission) { a.ActivationID = a.Parent.ID },
		"fabricated_activation_revision": func(a *Admission) { a.ActivationRevision = 1 },
		"fabricated_publication":         func(a *Admission) { a.ContextPublicationGeneration = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := deployment
			edit(&bad)
			if bad.Validate() == nil {
				t.Fatal("mixed or incomplete deployment evidence accepted")
			}
		})
	}
	for _, edit := range []func(*Admission){
		func(a *Admission) { a.Parent.ID = uuid.NewString() },
		func(a *Admission) { a.Parent.Revision++ },
		func(a *Admission) { a.ContextPublicationGeneration = 0 },
	} {
		bad := explicit
		edit(&bad)
		if bad.Validate() == nil {
			t.Fatal("contradictory runtime parent accepted")
		}
	}
}

func TestNativeInboxEntryContractUsesCompiledPlan(t *testing.T) {
	first, err := plangeneration.FromCanonicalValue(map[string]any{"connector": "a"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := plangeneration.FromCanonicalValue(map[string]any{"connector": "b"})
	if err != nil {
		t.Fatal(err)
	}
	a, err := EntryContractHash(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EntryContractHash(first)
	if err != nil || a != b {
		t.Fatalf("same compiled plan contract = %q, %v", b, err)
	}
	c, err := EntryContractHash(second)
	if err != nil || c == a {
		t.Fatalf("different compiled plan contract = %q, %v", c, err)
	}
}

func TestNativeInboxInstallOperationIsExactGeneration(t *testing.T) {
	setting := uuid.NewString()
	first, err := InstallOperationID(setting, 1)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := InstallOperationID(setting, 1)
	if err != nil || replay != first {
		t.Fatalf("same generation operation = %q, %v", replay, err)
	}
	next, err := InstallOperationID(setting, 2)
	if err != nil || next == first {
		t.Fatalf("next generation operation = %q, %v", next, err)
	}
	if _, err := InstallOperationID(setting, 0); err == nil {
		t.Fatal("zero generation accepted")
	}
	if _, err := InstallOperationID("not-uuid", 1); err == nil {
		t.Fatal("non-setting identity accepted")
	}
}

func TestNativeInboxEntryCommandFencesOldGeneration(t *testing.T) {
	setting := uuid.NewString()
	first, err := EntryCommand(setting, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) > 32 || first[:6] != "inbox_" {
		t.Fatalf("invalid provider command spelling %q", first)
	}
	replay, err := EntryCommand(setting, 1)
	if err != nil || replay != first {
		t.Fatalf("same generation command = %q, %v", replay, err)
	}
	next, err := EntryCommand(setting, 2)
	if err != nil || next == first {
		t.Fatalf("successor command = %q, %v", next, err)
	}
	other, err := EntryCommand(uuid.NewString(), 1)
	if err != nil || other == first {
		t.Fatalf("foreign setting command = %q, %v", other, err)
	}
	if _, err := DesiredCommands(setting, 0); err == nil {
		t.Fatal("zero generation produced a command list")
	}
}

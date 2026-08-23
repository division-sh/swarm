package bootverify

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"gopkg.in/yaml.v3"
)

func TestPolicyValueRowCarrierUsesQualifiedElementIdentity(t *testing.T) {
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`rules:
  lookup:
    element_id: 00000000-0000-4000-8000-000000000412
    lookup:
      on: payload.kind
      entries: [{key: service, value: selected}]
      into: computed.choice
      default: fail
`), &handler); err != nil {
		t.Fatal(err)
	}
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("flows/scout", "scout", "router")
	if err != nil {
		t.Fatal(err)
	}
	handler, err = runtimecontracts.QualifySystemNodeHandlerRuleRefs(node, handler)
	if err != nil {
		t.Fatal(err)
	}
	ref := policyRuleElementRef(handler.Rules[0])
	if !ref.Valid() || ref.PackageKey().String() != "flows/scout" || ref.ElementID().String() != "00000000-0000-4000-8000-000000000412" {
		t.Fatalf("policy rule ref = %#v", ref)
	}
}

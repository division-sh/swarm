package bootverify

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"gopkg.in/yaml.v3"
)

func TestPolicyValueRowCarrierUsesDeclarationIdentity(t *testing.T) {
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`rules:
  lookup:
    lookup:
      on: payload.kind
      entries: [{key: service, value: selected}]
      into: computed.choice
      default: fail
`), &handler); err != nil {
		t.Fatal(err)
	}
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("scout", "router")
	if err != nil {
		t.Fatal(err)
	}
	handler, err = runtimecontracts.QualifySystemNodeHandlerRuleRefsForEvent(node, "route.requested", handler)
	if err != nil {
		t.Fatal(err)
	}
	ref := policyRuleElementRef(handler.Rules[0])
	if !ref.Valid() || ref.Flow().String() != "scout" || ref.Family() != "handler_rule" || ref.SemanticPath() != `nodes["router"].handlers["route.requested"].rules[0]` {
		t.Fatalf("policy rule ref = %#v", ref)
	}
}

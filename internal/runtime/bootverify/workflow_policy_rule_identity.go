package bootverify

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func policyRuleElementRef(rule runtimecontracts.HandlerRuleEntry) runtimeidentity.DeclarationIdentity {
	ref, _ := rule.DeclarationIdentity()
	return ref
}

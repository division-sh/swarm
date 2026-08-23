package bootverify

import (
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
)

func policyRuleElementRef(rule runtimecontracts.HandlerRuleEntry) contractelementidentity.ContractElementRef {
	ref, _ := rule.ContractElementRef()
	return ref
}

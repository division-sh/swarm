package runtimepersistence

import (
	"github.com/division-sh/swarm/internal/operatorread"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

var agentIdentityFields = storeagent.IdentityFields

func IsAgentTargetAmbiguous(err error) bool {
	return operatorread.IsAgentTargetAmbiguous(err)
}

var _ func(runtimeagentidentity.Identity) (runtimeagentidentity.StorageFields, error) = agentIdentityFields

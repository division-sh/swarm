package runtimepersistence

import (
	"errors"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
)

var agentIdentityFields = storeagent.IdentityFields
var agentIdentityFromColumns = storeagent.IdentityFromColumns

func IsAgentTargetAmbiguous(err error) bool {
	return errors.Is(err, ErrAgentTargetAmbiguous)
}

var _ func(runtimeagentidentity.Identity) (runtimeagentidentity.StorageFields, error) = agentIdentityFields

package authority

import "github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"

type Account = authorityfact.Account
type Admission = authorityfact.Admission
type Claim = authorityfact.Claim
type NonClaim = authorityfact.NonClaim
type ParentScope = authorityfact.ParentScope

const (
	RuntimeParent    = authorityfact.RuntimeParent
	DeploymentParent = authorityfact.DeploymentParent
)

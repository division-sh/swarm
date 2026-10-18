package execution

import "github.com/division-sh/swarm/internal/sessionprovider/internal/executionfact"

// Channel exposes execution without a constructor, SDK client or mutable owner.
type Channel = executionfact.Channel

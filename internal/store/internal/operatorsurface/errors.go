package operatorsurface

import "github.com/division-sh/swarm/internal/store/internal/storeerrors"

var ErrRunNotFound = storeerrors.ErrRunNotFound
var ErrInvalidRunListCursor = storeerrors.ErrInvalidRunListCursor
var ErrEventNotFound = storeerrors.ErrEventNotFound
var ErrInvalidObservabilityCursor = storeerrors.ErrInvalidObservabilityCursor
var ErrEntityNotFound = storeerrors.ErrEntityNotFound
var ErrAgentNotFound = storeerrors.ErrAgentNotFound
var ErrSessionNotFound = storeerrors.ErrSessionNotFound
var ErrTurnNotFound = storeerrors.ErrTurnNotFound
var ErrAmbiguousEntityRunID = storeerrors.ErrAmbiguousEntityRunID
var ErrInvalidEntityCursor = storeerrors.ErrInvalidEntityCursor
var ErrInvalidConversationCursor = storeerrors.ErrInvalidConversationCursor
var ErrInvalidPendingAgentDeliveryCursor = storeerrors.ErrInvalidPendingAgentDeliveryCursor
var ErrInvalidEntityReadParam = storeerrors.ErrInvalidEntityReadParam

type EntityReadParamError = storeerrors.EntityReadParamError

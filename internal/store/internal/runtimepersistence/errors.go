package runtimepersistence

import (
	storeevent "github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	"github.com/division-sh/swarm/internal/store/internal/storeerrors"
)

var ErrUnknownSchemaType = storeerrors.ErrUnknownSchemaType
var ErrRunNotFound = storeerrors.ErrRunNotFound
var ErrInvalidRunListCursor = storeerrors.ErrInvalidRunListCursor
var ErrBundleNotFound = storeerrors.ErrBundleNotFound
var ErrInvalidBundleCatalogCursor = storeerrors.ErrInvalidBundleCatalogCursor
var ErrBundleCatalogConflict = storeerrors.ErrBundleCatalogConflict
var ErrEventNotFound = storeerrors.ErrEventNotFound
var ErrInvalidObservabilityCursor = storeerrors.ErrInvalidObservabilityCursor
var ErrEntityNotFound = storeerrors.ErrEntityNotFound
var ErrAgentNotFound = storeerrors.ErrAgentNotFound
var ErrAgentTargetAmbiguous = storeerrors.ErrAgentTargetAmbiguous
var ErrSessionNotFound = storeerrors.ErrSessionNotFound
var ErrTurnNotFound = storeerrors.ErrTurnNotFound
var ErrConversationForkNotFound = storeerrors.ErrConversationForkNotFound
var ErrInvalidConversationForkCursor = storeerrors.ErrInvalidConversationForkCursor
var ErrAmbiguousEntityRunID = storeerrors.ErrAmbiguousEntityRunID
var ErrInvalidEntityCursor = storeerrors.ErrInvalidEntityCursor
var ErrInvalidConversationCursor = storeerrors.ErrInvalidConversationCursor
var ErrInvalidPendingAgentDeliveryCursor = storeerrors.ErrInvalidPendingAgentDeliveryCursor
var ErrInvalidEntityReadParam = storeerrors.ErrInvalidEntityReadParam
var ErrEventIdentityConflict = storeevent.ErrEventIdentityConflict

type EntityReadParamError = storeerrors.EntityReadParamError

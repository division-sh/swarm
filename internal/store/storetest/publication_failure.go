package storetest

import private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"

type PublicationFailure = private.PublicationFailure
type PublicationFailureCut = private.PublicationFailureCut

const (
	PublicationAfterMaterialization = private.PublicationAfterMaterialization
	PublicationBeforeReplayScope    = private.PublicationBeforeReplayScope
	PublicationBeforeAPICompletion  = private.PublicationBeforeAPICompletion
)

func NewPublicationFailure(selected any, cut PublicationFailureCut, template string) (*PublicationFailure, error) {
	return private.NewPublicationFailureForTest(selected, cut, template)
}

package joinruntime

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

// WithForkReference consumes the fork owner's admitted history projection.
// It changes no business output, deadline, or retained lifecycle disposition.
func (a Activation) WithForkReference(ref timeridentity.JoinRef) (Activation, error) {
	if err := a.Validate(); err != nil {
		return Activation{}, err
	}
	if !ref.Valid() || ref.StageEntry().Empty() || !a.JoinRef().Declaration().Equal(ref.Declaration()) {
		return Activation{}, fmt.Errorf("fork join reference must preserve its exact arrival declaration")
	}
	var err error
	a.handle, err = joinHandleForKind(a.handle.Kind(), ref)
	if err != nil {
		return Activation{}, err
	}
	return a, a.Validate()
}

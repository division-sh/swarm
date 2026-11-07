package effectpersistence

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
)

func validateExternalEffectStoredScope(kind, id string, evidence []byte, bundle sql.NullString) (channelnative.TransportParent, error) {
	parent, err := authoractivity.DeploymentEffectParent(kind, id, evidence)
	if err != nil {
		return channelnative.TransportParent{}, err
	}
	if parent != nil {
		if bundle.Valid {
			return channelnative.TransportParent{}, fmt.Errorf("deployment effect cannot borrow an author bundle")
		}
	} else if !bundle.Valid || strings.TrimSpace(bundle.String) == "" {
		return channelnative.TransportParent{}, fmt.Errorf("authored effect requires its original bundle")
	}
	if parent == nil {
		return channelnative.TransportParent{}, nil
	}
	return *parent, nil
}

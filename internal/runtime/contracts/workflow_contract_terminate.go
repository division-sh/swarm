package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectTransitionTerminate(fields map[string]yamlsource.Value, target string) (bool, error) {
	value, present := fields["terminate"]
	if !present {
		return false, nil
	}
	terminate, err := nodeValueBool(value, "terminate")
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(target) == "" {
		return false, nodeValueError(value, fmt.Errorf("terminate is a transition property and requires advances_to"))
	}
	return terminate, nil
}

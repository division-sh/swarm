package authorityfact

import "fmt"

// ParentScope selects the owning connect lifecycle, never a runtime fallback.
type ParentScope string

const (
	RuntimeParent    ParentScope = ""
	DeploymentParent ParentScope = "deployment"
)

func (s ParentScope) Validate() error {
	if s != RuntimeParent && s != DeploymentParent {
		return fmt.Errorf("unknown channel operation parent scope")
	}
	return nil
}

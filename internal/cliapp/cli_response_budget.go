package cliapp

import (
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/apiv1"
)

// responseBudgetCatalog is the lazily loaded method catalog used to derive
// budget-error remediation mechanically from the one owner of read-surface
// shape (the embedded platform spec), never from a hand-written map.
var responseBudgetCatalog = struct {
	sync.Once
	registry *apiv1.Registry
	err      error
}{}

func loadResponseBudgetCatalog() *apiv1.Registry {
	responseBudgetCatalog.Do(func() {
		specPath, err := EmbeddedPlatformSpecPath()
		if err != nil {
			responseBudgetCatalog.err = err
			return
		}
		responseBudgetCatalog.registry, responseBudgetCatalog.err = apiv1.LoadRegistry(specPath)
	})
	return responseBudgetCatalog.registry
}

func responseBudgetRemediation(method string) string {
	registry := loadResponseBudgetCatalog()
	if registry == nil {
		return "the method catalog is unavailable; report the endpoint as unbounded by contract"
	}
	declared, ok := registry.Method(method)
	if !ok {
		return "the method is absent from the method catalog; report the endpoint as unbounded by contract"
	}
	bounded := make([]string, 0, 2)
	for _, param := range declared.Params {
		name := strings.TrimSpace(param.Name)
		if name == "limit" || name == "cursor" || strings.HasSuffix(name, "_limit") || strings.HasSuffix(name, "_cursor") {
			bounded = append(bounded, name)
		}
	}
	if len(bounded) == 0 {
		return "this endpoint has no bounded form; server contract gap — see the #2016 census"
	}
	return "use the bounded form: the " + method + " RPC accepts " + strings.Join(bounded, ", ") + " params for scoped/paginated reads"
}

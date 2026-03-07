package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

type guardActionRegistryDocument struct {
	Guards []struct {
		ID string `yaml:"id"`
	} `yaml:"guards"`
	Actions []struct {
		ID string `yaml:"id"`
	} `yaml:"actions"`
}

type contractIDRegistry struct {
	ids map[string]struct{}
}

func (r contractIDRegistry) has(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	_, ok := r.ids[id]
	return ok
}

func (r contractIDRegistry) sortedIDs() []string {
	out := make([]string, 0, len(r.ids))
	for id := range r.ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

type contractGuardRegistry struct {
	registry contractIDRegistry
}

func (r contractGuardRegistry) HasGuard(id string) bool { return r.registry.has(id) }
func (r contractGuardRegistry) GuardIDs() []string      { return r.registry.sortedIDs() }

type contractActionRegistry struct {
	registry contractIDRegistry
}

func (r contractActionRegistry) HasAction(id string) bool { return r.registry.has(id) }
func (r contractActionRegistry) ActionIDs() []string      { return r.registry.sortedIDs() }

var (
	workflowGuardRegistryOnce sync.Once
	workflowGuardRegistry     contractGuardRegistry
	workflowActionRegistry    contractActionRegistry
	workflowRegistryErr       error
)

func empireGuardRegistry() GuardRegistry {
	loadWorkflowGuardActionRegistries()
	if workflowRegistryErr != nil {
		panic(workflowRegistryErr)
	}
	return workflowGuardRegistry
}

func empireActionRegistry() ActionRegistry {
	loadWorkflowGuardActionRegistries()
	if workflowRegistryErr != nil {
		panic(workflowRegistryErr)
	}
	return workflowActionRegistry
}

func loadWorkflowGuardActionRegistries() {
	workflowGuardRegistryOnce.Do(func() {
		path := filepath.Join(workflowRepoRoot(), "contracts", "guard-action-registry.yaml")
		raw, err := os.ReadFile(path)
		if err != nil {
			workflowRegistryErr = fmt.Errorf("read %s: %w", path, err)
			return
		}
		var doc guardActionRegistryDocument
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			workflowRegistryErr = fmt.Errorf("parse %s: %w", path, err)
			return
		}
		guards := make(map[string]struct{}, len(doc.Guards))
		for _, guard := range doc.Guards {
			id := strings.TrimSpace(guard.ID)
			if id != "" {
				guards[id] = struct{}{}
			}
		}
		actions := make(map[string]struct{}, len(doc.Actions))
		for _, action := range doc.Actions {
			id := strings.TrimSpace(action.ID)
			if id != "" {
				actions[id] = struct{}{}
			}
		}
		workflowGuardRegistry = contractGuardRegistry{registry: contractIDRegistry{ids: guards}}
		workflowActionRegistry = contractActionRegistry{registry: contractIDRegistry{ids: actions}}
	})
}

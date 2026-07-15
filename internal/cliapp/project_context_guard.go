package cliapp

import (
	"context"
	"fmt"
	"strings"
)

func guardServeProjectContext(ctx context.Context, registry localContextRegistry, project cliProjectResolution, contextName string, explicitContext bool) error {
	allEntries, err := registry.ListDescriptors()
	if err != nil {
		return fmt.Errorf("inspect context registry: %w", err)
	}
	entries, err := registry.ProjectEntries(ctx, project.canonicalProjectRoot, cliRuntimeIdentityCaller{})
	if err != nil {
		return fmt.Errorf("inspect project contexts: %w", err)
	}
	if explicitContext {
		existing, exists := serveProjectContextEntryByName(allEntries, contextName)
		if !exists {
			return nil
		}
		if projectEntry, sameProject := serveProjectContextEntryByName(entries, contextName); sameProject {
			if serveProjectContextEntryReclaimable(projectEntry) {
				if err := registry.DeleteDescriptor(projectEntry.Descriptor.Name); err != nil {
					return fmt.Errorf("reclaim stale project context %s: %w", contextName, err)
				}
				return nil
			}
			return fmt.Errorf("context %s already exists for project %s (%s); run `swarm context prune` after confirming stale entries, or choose another --context", contextName, project.canonicalProjectRoot, projectEntry.Status)
		}
		existingProject := strings.TrimSpace(existing.Descriptor.ProjectRoot)
		if existingProject == "" {
			existingProject = "<unknown>"
		}
		return fmt.Errorf("context %s already exists for project %s (%s); context names are global, choose another --context", contextName, existingProject, existing.Status)
	}
	if len(entries) == 0 {
		return nil
	}
	blockers := make([]localContextEntry, 0, len(entries))
	reclaimable := make([]localContextEntry, 0, len(entries))
	for _, entry := range entries {
		if serveProjectContextEntryReclaimable(entry) {
			reclaimable = append(reclaimable, entry)
			continue
		}
		blockers = append(blockers, entry)
	}
	if len(blockers) == 0 {
		for _, entry := range reclaimable {
			if err := registry.DeleteDescriptor(entry.Descriptor.Name); err != nil {
				return fmt.Errorf("reclaim stale project context %s: %w", entry.Descriptor.Name, err)
			}
		}
		return nil
	}
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Descriptor.Name)
		if name == "" {
			name = "<unknown>"
		}
		parts = append(parts, fmt.Sprintf("%s=%s", name, entry.Status))
	}
	return fmt.Errorf("project %s already has context descriptors (%s); refusing bare `swarm serve --dev` to avoid orphaning a runtime; run `swarm context prune` for stale entries or pass --context for an intentional second runtime", project.canonicalProjectRoot, strings.Join(parts, ", "))
}

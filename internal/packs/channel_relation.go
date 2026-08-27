package packs

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// compiledChannelMappingTopology is the single target topology validated by
// the compiler and consumed by operation preparation.
type compiledChannelMappingTopology struct {
	Targets     []string
	ItemTargets map[string][]string
}

func compileChannelMappingTopology(subject string, mappings map[string]ChannelMapping) (compiledChannelMappingTopology, error) {
	topology := compiledChannelMappingTopology{ItemTargets: map[string][]string{}}
	targets := newChannelPathCardinality(subject + " target")
	for _, target := range sortedKeys(mappings) {
		if err := targets.add(target); err != nil {
			return compiledChannelMappingTopology{}, err
		}
		topology.Targets = append(topology.Targets, target)
		mapping := mappings[target]
		if mapping.Each == "" {
			continue
		}
		if len(mapping.Item) != 1 {
			return compiledChannelMappingTopology{}, fmt.Errorf("%s %q must construct exactly one item object", subject, target)
		}
		itemTargets := newChannelPathCardinality(subject + " " + target + " item target")
		for _, itemTarget := range sortedKeys(mapping.Item[0]) {
			if err := itemTargets.add(itemTarget); err != nil {
				return compiledChannelMappingTopology{}, err
			}
			topology.ItemTargets[target] = append(topology.ItemTargets[target], itemTarget)
		}
	}
	return topology, nil
}

type channelPathCardinality struct {
	subject string
	paths   []string
}

func newChannelPathCardinality(subject string) *channelPathCardinality {
	return &channelPathCardinality{subject: strings.TrimSpace(subject)}
}

func (c *channelPathCardinality) add(path string) error {
	path = strings.TrimSpace(path)
	for _, existing := range c.paths {
		if channelPathsOverlap(existing, path) {
			return fmt.Errorf("%s path %q overlaps %q; each semantic path must be used exactly once", c.subject, path, existing)
		}
	}
	c.paths = append(c.paths, path)
	sort.Strings(c.paths)
	return nil
}

func (c *channelPathCardinality) values() []string {
	return append([]string(nil), c.paths...)
}

func validateRequiredPathCardinality(subject string, required, mapped []string) error {
	for _, path := range required {
		count := 0
		for _, candidate := range mapped {
			if channelPathCovers(candidate, path) {
				count++
			}
		}
		if count != 1 {
			return fmt.Errorf("%s required path %q is covered %d times; require exactly once", subject, path, count)
		}
	}
	return nil
}

func channelPathCovers(mapped, required string) bool {
	mappedParts := channelPathParts(mapped)
	requiredParts := channelPathParts(required)
	if len(mappedParts) == 0 || len(mappedParts) > len(requiredParts) {
		return len(mappedParts) == 0 && len(requiredParts) == 0
	}
	for index := range mappedParts {
		if mappedParts[index] != requiredParts[index] {
			return false
		}
	}
	return true
}

func channelPathsOverlap(left, right string) bool {
	return channelPathCovers(left, right) || channelPathCovers(right, left)
}

func channelPathParts(path string) []string {
	path = strings.ReplaceAll(strings.TrimSpace(path), "[]", ".[]")
	parts := strings.Split(path, ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func validateDirectionalRelation(subject string, source, target *runtimecontracts.ToolInputSchema) error {
	if source == nil || target == nil {
		return fmt.Errorf("%s has no source or target schema", subject)
	}
	return source.ValidateAssignableTo(subject, *target)
}

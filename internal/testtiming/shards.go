package testtiming

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

const (
	ShardSnapshotVersion = 1
	defaultPackageWeight = 1.0
	minPackageWeight     = 0.1
)

type ShardSnapshot struct {
	Version      int            `json:"version"`
	Source       string         `json:"source"`
	ShardCount   int            `json:"shard_count"`
	MaxImbalance float64        `json:"max_imbalance"`
	Shards       []PackageShard `json:"shards"`
}

type PackageShard struct {
	ID       int      `json:"id"`
	Weight   float64  `json:"weight"`
	Packages []string `json:"packages"`
}

type ShardValidation struct {
	Missing        []string
	Extra          []string
	Duplicates     []string
	MaxWeight      float64
	MinWeight      float64
	ImbalanceRatio float64
}

func ParsePackageWeights(r io.Reader) (map[string]float64, error) {
	if r == nil {
		return nil, fmt.Errorf("input reader is nil")
	}
	weights := map[string]float64{}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var evt event
		if err := json.Unmarshal([]byte(line), &evt); err != nil {
			continue
		}
		if !isTerminalAction(strings.TrimSpace(evt.Action)) {
			continue
		}
		pkg := strings.TrimSpace(evt.Package)
		if pkg == "" || strings.TrimSpace(evt.Test) != "" {
			continue
		}
		weights[pkg] = normalizedPackageWeight(evt.Elapsed)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read test JSON: %w", err)
	}
	return weights, nil
}

func BuildShardSnapshot(packages []string, weights map[string]float64, shardCount int, source string, maxImbalance float64) (ShardSnapshot, error) {
	normalized, err := normalizePackageList(packages)
	if err != nil {
		return ShardSnapshot{}, err
	}
	if shardCount <= 0 {
		return ShardSnapshot{}, fmt.Errorf("shard count must be positive")
	}
	if len(normalized) == 0 {
		return ShardSnapshot{}, fmt.Errorf("package list is empty")
	}
	shards := make([]PackageShard, shardCount)
	for i := range shards {
		shards[i].ID = i + 1
	}
	items := make([]weightedPackage, 0, len(normalized))
	for _, pkg := range normalized {
		weight := defaultPackageWeight
		if weights != nil {
			if measured, ok := weights[pkg]; ok {
				weight = normalizedPackageWeight(measured)
			}
		}
		items = append(items, weightedPackage{Package: pkg, Weight: weight})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Weight != items[j].Weight {
			return items[i].Weight > items[j].Weight
		}
		return items[i].Package < items[j].Package
	})
	for _, item := range items {
		target := lightestShard(shards)
		shards[target].Packages = append(shards[target].Packages, item.Package)
		shards[target].Weight += item.Weight
	}
	for i := range shards {
		sort.Strings(shards[i].Packages)
	}
	return ShardSnapshot{
		Version:      ShardSnapshotVersion,
		Source:       strings.TrimSpace(source),
		ShardCount:   shardCount,
		MaxImbalance: maxImbalance,
		Shards:       shards,
	}, nil
}

func ValidateShardSnapshot(snapshot ShardSnapshot, packages []string) (ShardValidation, error) {
	if snapshot.Version != ShardSnapshotVersion {
		return ShardValidation{}, fmt.Errorf("unsupported shard snapshot version %d", snapshot.Version)
	}
	if snapshot.ShardCount != len(snapshot.Shards) {
		return ShardValidation{}, fmt.Errorf("shard_count = %d, want %d shard entries", snapshot.ShardCount, len(snapshot.Shards))
	}
	expected, err := normalizePackageList(packages)
	if err != nil {
		return ShardValidation{}, err
	}
	expectedSet := map[string]struct{}{}
	for _, pkg := range expected {
		expectedSet[pkg] = struct{}{}
	}
	seen := map[string]int{}
	validation := ShardValidation{}
	for _, shard := range snapshot.Shards {
		if shard.ID <= 0 {
			return ShardValidation{}, fmt.Errorf("shard id must be positive")
		}
		if validation.MaxWeight == 0 || shard.Weight > validation.MaxWeight {
			validation.MaxWeight = shard.Weight
		}
		if validation.MinWeight == 0 || shard.Weight < validation.MinWeight {
			validation.MinWeight = shard.Weight
		}
		for _, pkg := range shard.Packages {
			trimmed := strings.TrimSpace(pkg)
			if trimmed == "" {
				return ShardValidation{}, fmt.Errorf("shard %d includes empty package", shard.ID)
			}
			seen[trimmed]++
		}
	}
	for pkg, count := range seen {
		if count > 1 {
			validation.Duplicates = append(validation.Duplicates, pkg)
		}
		if _, ok := expectedSet[pkg]; !ok {
			validation.Extra = append(validation.Extra, pkg)
		}
	}
	for _, pkg := range expected {
		if seen[pkg] == 0 {
			validation.Missing = append(validation.Missing, pkg)
		}
	}
	sort.Strings(validation.Duplicates)
	sort.Strings(validation.Extra)
	sort.Strings(validation.Missing)
	if validation.MaxWeight > 0 {
		validation.ImbalanceRatio = (validation.MaxWeight - validation.MinWeight) / validation.MaxWeight
	}
	if len(validation.Missing) > 0 || len(validation.Extra) > 0 || len(validation.Duplicates) > 0 {
		return validation, fmt.Errorf("shard snapshot does not match package list")
	}
	return validation, nil
}

func ShardMatrixJSON(snapshot ShardSnapshot) ([]byte, error) {
	type shardEntry struct {
		Shard int `json:"shard"`
	}
	matrix := struct {
		Include []shardEntry `json:"include"`
	}{Include: make([]shardEntry, 0, len(snapshot.Shards))}
	for _, shard := range snapshot.Shards {
		matrix.Include = append(matrix.Include, shardEntry{Shard: shard.ID})
	}
	return json.Marshal(matrix)
}

func PackagesForShard(snapshot ShardSnapshot, shardID int) ([]string, error) {
	for _, shard := range snapshot.Shards {
		if shard.ID == shardID {
			return append([]string(nil), shard.Packages...), nil
		}
	}
	return nil, fmt.Errorf("shard %d not found", shardID)
}

func ReadPackageList(r io.Reader) ([]string, error) {
	if r == nil {
		return nil, fmt.Errorf("package list reader is nil")
	}
	var packages []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		pkg := strings.TrimSpace(scanner.Text())
		if pkg != "" {
			packages = append(packages, pkg)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read package list: %w", err)
	}
	return normalizePackageList(packages)
}

type weightedPackage struct {
	Package string
	Weight  float64
}

func normalizePackageList(packages []string) ([]string, error) {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(packages))
	for _, pkg := range packages {
		trimmed := strings.TrimSpace(pkg)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			return nil, fmt.Errorf("duplicate package %q", trimmed)
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out, nil
}

func normalizedPackageWeight(weight float64) float64 {
	if weight < minPackageWeight {
		return minPackageWeight
	}
	return weight
}

func lightestShard(shards []PackageShard) int {
	target := 0
	for i := 1; i < len(shards); i++ {
		if shards[i].Weight < shards[target].Weight {
			target = i
			continue
		}
		if shards[i].Weight == shards[target].Weight && shards[i].ID < shards[target].ID {
			target = i
		}
	}
	return target
}

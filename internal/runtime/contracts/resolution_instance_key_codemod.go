package contracts

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type RetiredResolutionInstanceKeyRewriteResult struct {
	Files        int
	Declarations int
}

type resolutionInstanceKeyRewrite struct {
	path string
	mode os.FileMode
	raw  []byte
}

// RewriteRetiredResolutionInstanceKeys removes deterministic legacy
// resolution.instance_key declarations after validating every schema first.
func RewriteRetiredResolutionInstanceKeys(contractsRoot string) (RetiredResolutionInstanceKeyRewriteResult, error) {
	contractsRoot = strings.TrimSpace(contractsRoot)
	if contractsRoot == "" {
		return RetiredResolutionInstanceKeyRewriteResult{}, fmt.Errorf("contracts root is required")
	}
	var schemaFiles []string
	if err := filepath.WalkDir(contractsRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && entry.Name() == "schema.yaml" {
			schemaFiles = append(schemaFiles, path)
		}
		return nil
	}); err != nil {
		return RetiredResolutionInstanceKeyRewriteResult{}, fmt.Errorf("scan contracts root %s: %w", contractsRoot, err)
	}
	sort.Strings(schemaFiles)

	result := RetiredResolutionInstanceKeyRewriteResult{}
	rewrites := make([]resolutionInstanceKeyRewrite, 0, len(schemaFiles))
	for _, path := range schemaFiles {
		rewrite, declarations, changed, err := prepareResolutionInstanceKeyRewrite(path)
		if err != nil {
			return RetiredResolutionInstanceKeyRewriteResult{}, err
		}
		if !changed {
			continue
		}
		result.Files++
		result.Declarations += declarations
		rewrites = append(rewrites, rewrite)
	}
	if err := writeResolutionInstanceKeyRewrites(rewrites); err != nil {
		return RetiredResolutionInstanceKeyRewriteResult{}, err
	}
	return result, nil
}

func prepareResolutionInstanceKeyRewrite(path string) (resolutionInstanceKeyRewrite, int, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return resolutionInstanceKeyRewrite{}, 0, false, fmt.Errorf("read %s: %w", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return resolutionInstanceKeyRewrite{}, 0, false, fmt.Errorf("stat %s: %w", path, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return resolutionInstanceKeyRewrite{}, 0, false, fmt.Errorf("decode %s: %w", path, err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return resolutionInstanceKeyRewrite{}, 0, false, fmt.Errorf("%s must contain one flow schema mapping", path)
	}
	root := document.Content[0]
	instance, err := uniqueYAMLMappingValue(root, "instance")
	if err != nil {
		return resolutionInstanceKeyRewrite{}, 0, false, fmt.Errorf("%s: %w", path, err)
	}
	events, err := flowInputEventRows(root)
	if err != nil {
		return resolutionInstanceKeyRewrite{}, 0, false, fmt.Errorf("%s: %w", path, err)
	}
	declarations := 0
	for index, event := range events {
		changed, err := rewriteResolutionInstanceKeyEvent(event, instance)
		if err != nil {
			return resolutionInstanceKeyRewrite{}, 0, false, fmt.Errorf("%s pins.inputs.events[%d]: %w", path, index, err)
		}
		if changed {
			declarations++
		}
	}
	if declarations == 0 {
		return resolutionInstanceKeyRewrite{}, 0, false, nil
	}
	var encoded bytes.Buffer
	encoder := yaml.NewEncoder(&encoded)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return resolutionInstanceKeyRewrite{}, 0, false, fmt.Errorf("encode %s: %w", path, err)
	}
	if err := encoder.Close(); err != nil {
		return resolutionInstanceKeyRewrite{}, 0, false, fmt.Errorf("finish encoding %s: %w", path, err)
	}
	return resolutionInstanceKeyRewrite{path: path, mode: info.Mode().Perm(), raw: encoded.Bytes()}, declarations, true, nil
}

func rewriteResolutionInstanceKeyEvent(event, instance *yaml.Node) (bool, error) {
	if event == nil || event.Kind != yaml.MappingNode {
		return false, fmt.Errorf("input event pin must be a mapping")
	}
	resolution, err := uniqueYAMLMappingValue(event, "resolution")
	if err != nil {
		return false, err
	}
	if resolution == nil {
		return false, nil
	}
	if resolution.Kind != yaml.MappingNode {
		return false, fmt.Errorf("resolution must be a mapping")
	}
	instanceKeyIndex, instanceKey, err := uniqueYAMLMappingEntry(resolution, "instance_key")
	if err != nil || instanceKey == nil {
		return false, err
	}
	by, err := flowInstanceIdentityField(instance)
	if err != nil {
		return false, err
	}
	if by == "" {
		return false, fmt.Errorf("resolution.instance_key requires a single flow instance.by owner")
	}
	modeNode, err := uniqueYAMLMappingValue(resolution, "mode")
	if err != nil {
		return false, err
	}
	mode, err := requiredYAMLScalar(modeNode, "resolution.mode")
	if err != nil {
		return false, err
	}
	carryFrom, err := flowIdentityCarrySource(event, by)
	if err != nil {
		return false, err
	}

	switch mode {
	case FlowInputResolutionModeCreate:
		mint, as, err := retiredCreateInstanceKey(instanceKey)
		if err != nil {
			return false, err
		}
		if as != by {
			return false, fmt.Errorf("resolution.instance_key.as %q does not match instance.by %q", as, by)
		}
		wantOldSource := "instance.key." + by
		if strings.TrimSpace(carryFrom.Value) != wantOldSource {
			return false, fmt.Errorf("carries.%s.from %q does not match deterministic legacy source %q", by, strings.TrimSpace(carryFrom.Value), wantOldSource)
		}
		switch mint {
		case "uuid":
			carryFrom.Value = FlowInputCarrySourceGeneratedUUID
		case "event_id":
			carryFrom.Value = FlowInputCarrySourceEventID
		default:
			return false, fmt.Errorf("resolution.instance_key.mint %q requires manual migration", mint)
		}
		carryFrom.Tag = "!!str"
	case FlowInputResolutionModeSelect, FlowInputResolutionModeSelectOrCreate:
		legacyField, err := retiredCarriedInstanceKey(instanceKey)
		if err != nil {
			return false, err
		}
		if legacyField != by {
			return false, fmt.Errorf("resolution.instance_key %q does not match instance.by %q", legacyField, by)
		}
		if !topLevelPayloadSource(carryFrom.Value) {
			return false, fmt.Errorf("carries.%s.from %q requires manual migration; selecting pins require one top-level payload source", by, strings.TrimSpace(carryFrom.Value))
		}
	default:
		return false, fmt.Errorf("resolution.mode %q with instance_key requires manual migration", mode)
	}
	resolution.Content = append(resolution.Content[:instanceKeyIndex], resolution.Content[instanceKeyIndex+2:]...)
	return true, nil
}

func flowInstanceIdentityField(instance *yaml.Node) (string, error) {
	if instance == nil {
		return "", nil
	}
	if instance.Kind != yaml.MappingNode {
		return "", fmt.Errorf("instance must be a mapping for retired instance_key migration")
	}
	byNode, err := uniqueYAMLMappingValue(instance, "by")
	if err != nil {
		return "", err
	}
	if byNode == nil {
		return "", nil
	}
	if byNode.Kind == yaml.ScalarNode {
		return requiredYAMLScalar(byNode, "instance.by")
	}
	if byNode.Kind == yaml.SequenceNode && len(byNode.Content) == 1 {
		return requiredYAMLScalar(byNode.Content[0], "instance.by[0]")
	}
	return "", fmt.Errorf("retired instance_key migration requires exactly one instance.by field")
}

func flowInputEventRows(root *yaml.Node) ([]*yaml.Node, error) {
	pins, err := uniqueYAMLMappingValue(root, "pins")
	if err != nil || pins == nil {
		return nil, err
	}
	inputs, err := uniqueYAMLMappingValue(pins, "inputs")
	if err != nil || inputs == nil {
		return nil, err
	}
	events, err := uniqueYAMLMappingValue(inputs, "events")
	if err != nil || events == nil {
		return nil, err
	}
	if events.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("pins.inputs.events must be a sequence")
	}
	return events.Content, nil
}

func flowIdentityCarrySource(event *yaml.Node, field string) (*yaml.Node, error) {
	carries, err := uniqueYAMLMappingValue(event, "carries")
	if err != nil {
		return nil, err
	}
	if carries == nil || carries.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("resolution.instance_key requires carries.%s", field)
	}
	carry, err := uniqueYAMLMappingValue(carries, field)
	if err != nil {
		return nil, err
	}
	if carry == nil || carry.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("resolution.instance_key requires carries.%s mapping", field)
	}
	from, err := uniqueYAMLMappingValue(carry, "from")
	if err != nil {
		return nil, err
	}
	if _, err := requiredYAMLScalar(from, "carries."+field+".from"); err != nil {
		return nil, err
	}
	return from, nil
}

func retiredCreateInstanceKey(node *yaml.Node) (string, string, error) {
	if node.Kind != yaml.MappingNode {
		return "", "", fmt.Errorf("create resolution.instance_key must be a {mint, as} mapping")
	}
	if err := requireOnlyYAMLMappingKeys(node, "mint", "as"); err != nil {
		return "", "", err
	}
	mintNode, err := uniqueYAMLMappingValue(node, "mint")
	if err != nil {
		return "", "", err
	}
	asNode, err := uniqueYAMLMappingValue(node, "as")
	if err != nil {
		return "", "", err
	}
	mint, err := requiredYAMLScalar(mintNode, "resolution.instance_key.mint")
	if err != nil {
		return "", "", err
	}
	as, err := requiredYAMLScalar(asNode, "resolution.instance_key.as")
	return mint, as, err
}

func retiredCarriedInstanceKey(node *yaml.Node) (string, error) {
	if node.Kind == yaml.ScalarNode {
		return requiredYAMLScalar(node, "resolution.instance_key")
	}
	if node.Kind != yaml.MappingNode {
		return "", fmt.Errorf("resolution.instance_key requires manual migration")
	}
	if err := requireOnlyYAMLMappingKeys(node, "from"); err != nil {
		return "", err
	}
	from, err := uniqueYAMLMappingValue(node, "from")
	if err != nil {
		return "", err
	}
	return requiredYAMLScalar(from, "resolution.instance_key.from")
}

func uniqueYAMLMappingEntry(node *yaml.Node, key string) (int, *yaml.Node, error) {
	if node == nil || node.Kind != yaml.MappingNode {
		return -1, nil, fmt.Errorf("%s owner must be a mapping", key)
	}
	index := -1
	var value *yaml.Node
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) != key {
			continue
		}
		if value != nil {
			return -1, nil, fmt.Errorf("%s is declared more than once", key)
		}
		index, value = i, node.Content[i+1]
	}
	return index, value, nil
}

func uniqueYAMLMappingValue(node *yaml.Node, key string) (*yaml.Node, error) {
	_, value, err := uniqueYAMLMappingEntry(node, key)
	return value, err
}

func requiredYAMLScalar(node *yaml.Node, label string) (string, error) {
	if node == nil || node.Kind != yaml.ScalarNode || strings.TrimSpace(node.Value) == "" {
		return "", fmt.Errorf("%s must be a non-empty scalar", label)
	}
	return strings.TrimSpace(node.Value), nil
}

func requireOnlyYAMLMappingKeys(node *yaml.Node, allowed ...string) error {
	set := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		set[key] = struct{}{}
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := set[key]; !ok {
			return fmt.Errorf("resolution.instance_key field %q requires manual migration", key)
		}
	}
	return nil
}

func writeResolutionInstanceKeyRewrites(rewrites []resolutionInstanceKeyRewrite) error {
	type stagedRewrite struct {
		target string
		temp   string
	}
	staged := make([]stagedRewrite, 0, len(rewrites))
	defer func() {
		for _, rewrite := range staged {
			_ = os.Remove(rewrite.temp)
		}
	}()
	for _, rewrite := range rewrites {
		tmp, err := os.CreateTemp(filepath.Dir(rewrite.path), ".schema.yaml.resolution-instance-key-*")
		if err != nil {
			return fmt.Errorf("stage %s: %w", rewrite.path, err)
		}
		tempName := tmp.Name()
		staged = append(staged, stagedRewrite{target: rewrite.path, temp: tempName})
		if err := tmp.Chmod(rewrite.mode); err != nil {
			tmp.Close()
			return fmt.Errorf("set staged mode for %s: %w", rewrite.path, err)
		}
		if _, err := tmp.Write(rewrite.raw); err != nil {
			tmp.Close()
			return fmt.Errorf("stage contents for %s: %w", rewrite.path, err)
		}
		if err := tmp.Close(); err != nil {
			return fmt.Errorf("close staged rewrite for %s: %w", rewrite.path, err)
		}
	}
	for _, rewrite := range staged {
		if err := os.Rename(rewrite.temp, rewrite.target); err != nil {
			return fmt.Errorf("replace %s: %w", rewrite.target, err)
		}
	}
	return nil
}

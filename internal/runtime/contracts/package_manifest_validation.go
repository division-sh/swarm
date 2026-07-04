package contracts

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	packageKeywordPattern  = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
	packageExtraKeyPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+/[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?$`)
	githubOwnerPattern     = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$`)
	githubRepoPattern      = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

func decodePackageKeywordsYAML(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("keywords must be a list of lowercase slug strings")
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(node.Content))
	for i, item := range node.Content {
		if item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
			return nil, fmt.Errorf("keywords[%d] must be a string", i)
		}
		keyword := strings.TrimSpace(item.Value)
		if keyword != item.Value || !packageKeywordPattern.MatchString(keyword) {
			return nil, fmt.Errorf("keywords[%d] must be a lowercase slug using letters, digits, and hyphen", i)
		}
		if _, ok := seen[keyword]; ok {
			return nil, fmt.Errorf("keywords[%d] duplicates %q", i, keyword)
		}
		seen[keyword] = struct{}{}
		out = append(out, keyword)
	}
	return out, nil
}

func decodePackageLicenseYAML(node *yaml.Node) (string, error) {
	license, err := decodePackageScalarString(node, "license")
	if err != nil || license == "" {
		return license, err
	}
	if _, ok := spdxLicenseIDs[license]; !ok {
		return "", fmt.Errorf("license %q is not a non-deprecated SPDX license identifier", license)
	}
	return license, nil
}

func decodePackageRepositoryYAML(node *yaml.Node) (string, error) {
	repository, err := decodePackageScalarString(node, "repository")
	if err != nil || repository == "" {
		return repository, err
	}
	u, err := url.Parse(repository)
	if err != nil {
		return "", fmt.Errorf("repository must be a GitHub HTTPS repository URL: %w", err)
	}
	if u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("repository must be exactly https://github.com/{owner}/{repo}")
	}
	if strings.Contains(u.EscapedPath(), "%") {
		return "", fmt.Errorf("repository path must not use URL escapes")
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || u.Path != "/"+parts[0]+"/"+parts[1] {
		return "", fmt.Errorf("repository must be exactly https://github.com/{owner}/{repo}")
	}
	if !githubOwnerPattern.MatchString(parts[0]) || !githubRepoPattern.MatchString(parts[1]) || strings.HasSuffix(parts[1], ".git") {
		return "", fmt.Errorf("repository must be exactly https://github.com/{owner}/{repo}")
	}
	return repository, nil
}

func decodePackageExtraYAML(node *yaml.Node) (map[string]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("extra must be a mapping of namespaced string keys to string values")
	}
	out := map[string]string{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valueNode := node.Content[i+1]
		if keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" {
			return nil, fmt.Errorf("extra keys must be strings")
		}
		key := strings.TrimSpace(keyNode.Value)
		if key != keyNode.Value || !packageExtraKeyPattern.MatchString(key) {
			return nil, fmt.Errorf("extra key %q must be namespaced as domain.tld/name", keyNode.Value)
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("extra key %q is duplicated", key)
		}
		if valueNode.Kind != yaml.ScalarNode || valueNode.Tag != "!!str" {
			return nil, fmt.Errorf("extra.%s must be a string", key)
		}
		out[key] = valueNode.Value
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func decodePackageScalarString(node *yaml.Node, field string) (string, error) {
	if node == nil || node.Kind == 0 {
		return "", nil
	}
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return "", fmt.Errorf("%s must be a string", field)
	}
	value := strings.TrimSpace(node.Value)
	if value != node.Value || value == "" {
		return "", fmt.Errorf("%s must be a non-empty trimmed string", field)
	}
	return value, nil
}

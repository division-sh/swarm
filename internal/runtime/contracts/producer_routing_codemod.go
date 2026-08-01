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

type producerRoutingRewrite struct {
	path string
	mode fs.FileMode
	raw  []byte
}

// RewriteRetiredProducerBroadcasts removes only deterministic broadcast:true
// declarations. Any target or other broadcast shape requires an author-owned
// topology decision, so the complete bundle remains byte-for-byte unchanged.
func RewriteRetiredProducerBroadcasts(contractsRoot string) (int, error) {
	contractsRoot = strings.TrimSpace(contractsRoot)
	if contractsRoot == "" {
		return 0, fmt.Errorf("contracts root is required")
	}
	var files []string
	err := filepath.WalkDir(contractsRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && entry.Name() == "nodes.yaml" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("scan %s: %w", contractsRoot, err)
	}
	sort.Strings(files)

	total := 0
	rewrites := make([]producerRoutingRewrite, 0, len(files))
	for _, path := range files {
		rewrite, removed, err := planRetiredProducerBroadcastRewrite(path)
		if err != nil {
			return 0, err
		}
		total += removed
		if removed > 0 {
			rewrites = append(rewrites, rewrite)
		}
	}
	for _, rewrite := range rewrites {
		if err := writeProducerRoutingRewrite(rewrite.path, rewrite.mode, rewrite.raw); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func planRetiredProducerBroadcastRewrite(path string) (producerRoutingRewrite, int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return producerRoutingRewrite{}, 0, fmt.Errorf("read %s: %w", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return producerRoutingRewrite{}, 0, fmt.Errorf("stat %s: %w", path, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return producerRoutingRewrite{}, 0, fmt.Errorf("decode %s: %w", path, err)
	}
	removed, err := removeDeterministicProducerBroadcasts(path, &document)
	if err != nil {
		return producerRoutingRewrite{}, 0, err
	}
	if removed == 0 {
		return producerRoutingRewrite{}, 0, nil
	}
	var encoded bytes.Buffer
	encoder := yaml.NewEncoder(&encoded)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return producerRoutingRewrite{}, 0, fmt.Errorf("encode %s: %w", path, err)
	}
	if err := encoder.Close(); err != nil {
		return producerRoutingRewrite{}, 0, fmt.Errorf("finish encoding %s: %w", path, err)
	}
	return producerRoutingRewrite{path: path, mode: info.Mode().Perm(), raw: encoded.Bytes()}, removed, nil
}

func removeDeterministicProducerBroadcasts(path string, node *yaml.Node) (int, error) {
	if node == nil {
		return 0, nil
	}
	removed := 0
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			if strings.TrimSpace(node.Content[i].Value) != "emit" {
				continue
			}
			emit := node.Content[i+1]
			if emit.Kind != yaml.MappingNode {
				continue
			}
			broadcastIndex := -1
			for j := 0; j+1 < len(emit.Content); j += 2 {
				key := strings.TrimSpace(emit.Content[j].Value)
				switch key {
				case "target":
					return 0, fmt.Errorf("%s contains emit.target; recipient topology requires manual migration", path)
				case "broadcast":
					if broadcastIndex >= 0 {
						return 0, fmt.Errorf("%s contains duplicate emit.broadcast fields", path)
					}
					value := emit.Content[j+1]
					if value.Kind != yaml.ScalarNode || value.Tag != "!!bool" || strings.ToLower(strings.TrimSpace(value.Value)) != "true" {
						return 0, fmt.Errorf("%s contains emit.broadcast that is not boolean true; manual migration is required", path)
					}
					broadcastIndex = j
				}
			}
			if broadcastIndex >= 0 {
				emit.Content = append(emit.Content[:broadcastIndex], emit.Content[broadcastIndex+2:]...)
				removed++
			}
		}
	}
	for _, child := range node.Content {
		count, err := removeDeterministicProducerBroadcasts(path, child)
		if err != nil {
			return 0, err
		}
		removed += count
	}
	return removed, nil
}

func writeProducerRoutingRewrite(path string, mode fs.FileMode, raw []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".nodes.yaml.producer-routing-*")
	if err != nil {
		return fmt.Errorf("create temporary nodes.yaml: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return fmt.Errorf("set temporary nodes.yaml mode: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return fmt.Errorf("write temporary nodes.yaml: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary nodes.yaml: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace nodes.yaml: %w", err)
	}
	return nil
}

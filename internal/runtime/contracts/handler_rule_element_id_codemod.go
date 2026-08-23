package contracts

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	"gopkg.in/yaml.v3"
)

type HandlerRuleElementIDMintResult struct {
	FilesChanged int
	IDsMinted    int
}

type handlerRuleElementIDRewrite struct {
	path string
	mode os.FileMode
	raw  []byte
	ids  int
}

// MintHandlerRuleElementIDs performs the explicit one-time authored corpus
// migration. It recognizes rule rows only beneath node event_handlers and
// never derives an ID from labels, indexes, paths, or declaration content.
func MintHandlerRuleElementIDs(root string) (HandlerRuleElementIDMintResult, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return HandlerRuleElementIDMintResult{}, fmt.Errorf("contracts path is required")
	}
	info, err := os.Stat(root)
	if err != nil {
		return HandlerRuleElementIDMintResult{}, fmt.Errorf("inspect contracts path: %w", err)
	}
	paths := []string{}
	if info.Mode().IsRegular() {
		paths = append(paths, root)
	} else {
		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == ".git" || entry.Name() == ".swarm" || entry.Name() == "node_modules" || entry.Name() == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			ext := strings.ToLower(filepath.Ext(path))
			base := strings.ToLower(filepath.Base(path))
			if (ext == ".yaml" || ext == ".yml") && (base == "nodes.yaml" || base == "nodes.yml") {
				paths = append(paths, path)
			}
			return nil
		})
		if err != nil {
			return HandlerRuleElementIDMintResult{}, fmt.Errorf("scan contracts path: %w", err)
		}
	}
	sort.Strings(paths)
	rewrites := make([]handlerRuleElementIDRewrite, 0)
	for _, path := range paths {
		rewrite, changed, err := prepareHandlerRuleElementIDRewrite(path)
		if err != nil {
			return HandlerRuleElementIDMintResult{}, err
		}
		if changed {
			rewrites = append(rewrites, rewrite)
		}
	}
	result := HandlerRuleElementIDMintResult{}
	for _, rewrite := range rewrites {
		if err := writeHandlerRuleElementIDRewrite(rewrite); err != nil {
			return HandlerRuleElementIDMintResult{}, err
		}
		result.FilesChanged++
		result.IDsMinted += rewrite.ids
	}
	return result, nil
}

func prepareHandlerRuleElementIDRewrite(path string) (handlerRuleElementIDRewrite, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return handlerRuleElementIDRewrite{}, false, fmt.Errorf("read %s: %w", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return handlerRuleElementIDRewrite{}, false, fmt.Errorf("stat %s: %w", path, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return handlerRuleElementIDRewrite{}, false, fmt.Errorf("decode %s: %w", path, err)
	}
	ids, err := mintHandlerRuleElementIDsInNode(&document)
	if err != nil {
		return handlerRuleElementIDRewrite{}, false, fmt.Errorf("%s: %w", path, err)
	}
	if ids == 0 {
		return handlerRuleElementIDRewrite{}, false, nil
	}
	var encoded bytes.Buffer
	encoder := yaml.NewEncoder(&encoded)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return handlerRuleElementIDRewrite{}, false, fmt.Errorf("encode %s: %w", path, err)
	}
	if err := encoder.Close(); err != nil {
		return handlerRuleElementIDRewrite{}, false, fmt.Errorf("finish encoding %s: %w", path, err)
	}
	return handlerRuleElementIDRewrite{path: path, mode: info.Mode().Perm(), raw: encoded.Bytes(), ids: ids}, true, nil
}

func mintHandlerRuleElementIDsInNode(node *yaml.Node) (int, error) {
	if node == nil {
		return 0, nil
	}
	count := 0
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			key, value := strings.TrimSpace(node.Content[index].Value), node.Content[index+1]
			if key == "event_handlers" {
				minted, err := mintEventHandlerRuleElementIDs(value)
				if err != nil {
					return 0, err
				}
				count += minted
				continue
			}
			minted, err := mintHandlerRuleElementIDsInNode(value)
			if err != nil {
				return 0, err
			}
			count += minted
		}
		return count, nil
	}
	for _, child := range node.Content {
		minted, err := mintHandlerRuleElementIDsInNode(child)
		if err != nil {
			return 0, err
		}
		count += minted
	}
	return count, nil
}

func mintEventHandlerRuleElementIDs(handlers *yaml.Node) (int, error) {
	if handlers == nil || handlers.Kind != yaml.MappingNode {
		return 0, fmt.Errorf("event_handlers must be a mapping")
	}
	count := 0
	for index := 0; index+1 < len(handlers.Content); index += 2 {
		handler := handlers.Content[index+1]
		if handler.Kind != yaml.MappingNode {
			return 0, fmt.Errorf("event handler %q must be a mapping", strings.TrimSpace(handlers.Content[index].Value))
		}
		for field := 0; field+1 < len(handler.Content); field += 2 {
			key, value := strings.TrimSpace(handler.Content[field].Value), handler.Content[field+1]
			var minted int
			var err error
			switch key {
			case "rules", "on_complete":
				minted, err = mintRuleCollectionElementIDs(value)
			case "join":
				minted, err = mintJoinOutcomeElementIDs(value)
			}
			if err != nil {
				return 0, fmt.Errorf("handler %q %s: %w", strings.TrimSpace(handlers.Content[index].Value), key, err)
			}
			count += minted
		}
	}
	return count, nil
}

func mintRuleCollectionElementIDs(node *yaml.Node) (int, error) {
	if node == nil || node.Kind == 0 || yamlNodeIsNull(node) {
		return 0, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		count := 0
		for index, row := range node.Content {
			minted, err := ensureRuleElementID(row, nil)
			if err != nil {
				return 0, fmt.Errorf("row %d: %w", index, err)
			}
			count += minted
		}
		return count, nil
	case yaml.MappingNode:
		shape, err := classifyHandlerRuleMapping(node)
		if err != nil {
			return 0, err
		}
		if shape == handlerRuleMappingSingleton {
			return ensureRuleElementID(node, nil)
		}
		count := 0
		for index := 0; index+1 < len(node.Content); index += 2 {
			minted, err := ensureRuleElementID(node.Content[index+1], nil)
			if err != nil {
				return 0, fmt.Errorf("row %q: %w", strings.TrimSpace(node.Content[index].Value), err)
			}
			count += minted
		}
		return count, nil
	default:
		return 0, fmt.Errorf("rule collection must use mapping rows")
	}
}

func mintJoinOutcomeElementIDs(join *yaml.Node) (int, error) {
	if join == nil || join.Kind != yaml.MappingNode {
		return 0, nil
	}
	count := 0
	for index := 0; index+1 < len(join.Content); index += 2 {
		key, value := strings.TrimSpace(join.Content[index].Value), join.Content[index+1]
		switch key {
		case "on_complete":
			minted, err := ensureRuleElementID(value, nil)
			if err != nil {
				return 0, err
			}
			count += minted
		case "timeout":
			minted, err := ensureRuleElementID(value, map[string]struct{}{"after": {}})
			if err != nil {
				return 0, err
			}
			count += minted
		}
	}
	return count, nil
}

func ensureRuleElementID(row *yaml.Node, ignored map[string]struct{}) (int, error) {
	if row == nil || row.Kind != yaml.MappingNode {
		return 0, fmt.Errorf("authored rule must be a mapping")
	}
	semanticFields := 0
	hasElementID := false
	for index := 0; index+1 < len(row.Content); index += 2 {
		key := strings.TrimSpace(row.Content[index].Value)
		if key == "element_id" {
			if hasElementID {
				return 0, fmt.Errorf("element_id may appear only once")
			}
			hasElementID = true
			value := row.Content[index+1]
			if value.Kind != yaml.ScalarNode {
				return 0, fmt.Errorf("element_id must be a scalar")
			}
			if _, err := contractelementidentity.ParseContractElementID(value.Value); err != nil {
				return 0, err
			}
			continue
		}
		if _, skip := ignored[key]; skip {
			continue
		}
		if _, supported := ruleFieldOptions[key]; !supported {
			return 0, fmt.Errorf("unsupported handler rule field %q", key)
		}
		semanticFields++
	}
	if hasElementID {
		return 0, nil
	}
	if semanticFields == 0 {
		return 0, nil
	}
	id := contractelementidentity.MintContractElementID()
	row.Content = append([]*yaml.Node{
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: "element_id"},
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: id.String()},
	}, row.Content...)
	return 1, nil
}

func writeHandlerRuleElementIDRewrite(rewrite handlerRuleElementIDRewrite) error {
	dir := filepath.Dir(rewrite.path)
	tmp, err := os.CreateTemp(dir, ".element-id-*")
	if err != nil {
		return fmt.Errorf("create temporary contract file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(rewrite.mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(rewrite.raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, rewrite.path); err != nil {
		return fmt.Errorf("replace %s: %w", rewrite.path, err)
	}
	return nil
}

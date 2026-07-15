package contracts

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// RewriteRetiredConnectDeliveryOne removes only the deterministic retired
// connect.delivery: one spelling. Other retired routing declarations require
// an author decision and leave the file untouched.
func RewriteRetiredConnectDeliveryOne(packageFile string) (int, error) {
	packageFile = strings.TrimSpace(packageFile)
	if packageFile == "" {
		return 0, fmt.Errorf("package.yaml path is required")
	}
	raw, err := os.ReadFile(packageFile)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", packageFile, err)
	}
	info, err := os.Stat(packageFile)
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", packageFile, err)
	}

	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return 0, fmt.Errorf("decode %s: %w", packageFile, err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return 0, fmt.Errorf("%s must contain one package mapping", packageFile)
	}

	root := document.Content[0]
	var connect *yaml.Node
	for i := 0; i+1 < len(root.Content); i += 2 {
		if strings.TrimSpace(root.Content[i].Value) != "connect" {
			continue
		}
		if connect != nil {
			return 0, fmt.Errorf("%s declares connect more than once", packageFile)
		}
		connect = root.Content[i+1]
	}
	if connect == nil {
		return 0, nil
	}
	if connect.Kind != yaml.SequenceNode {
		return 0, fmt.Errorf("%s connect must be a sequence", packageFile)
	}

	type removal struct {
		row   *yaml.Node
		index int
	}
	removals := make([]removal, 0, len(connect.Content))
	for rowIndex, row := range connect.Content {
		if row.Kind != yaml.MappingNode {
			return 0, fmt.Errorf("%s connect[%d] must be a mapping", packageFile, rowIndex)
		}
		deliveryIndex := -1
		for i := 0; i+1 < len(row.Content); i += 2 {
			key := strings.TrimSpace(row.Content[i].Value)
			switch key {
			case "reply":
				return 0, fmt.Errorf("%s connect[%d].reply requires manual migration to receiver resolution.mode: reply", packageFile, rowIndex)
			case "delivery":
				if deliveryIndex >= 0 {
					return 0, fmt.Errorf("%s connect[%d] declares delivery more than once", packageFile, rowIndex)
				}
				value := row.Content[i+1]
				if value.Kind != yaml.ScalarNode || strings.TrimSpace(value.Value) != "one" {
					return 0, fmt.Errorf("%s connect[%d].delivery requires manual migration; this codemod only removes delivery: one", packageFile, rowIndex)
				}
				deliveryIndex = i
			}
		}
		if deliveryIndex >= 0 {
			removals = append(removals, removal{row: row, index: deliveryIndex})
		}
	}
	if len(removals) == 0 {
		return 0, nil
	}
	for _, item := range removals {
		item.row.Content = append(item.row.Content[:item.index], item.row.Content[item.index+2:]...)
	}

	var encoded bytes.Buffer
	encoder := yaml.NewEncoder(&encoded)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return 0, fmt.Errorf("encode %s: %w", packageFile, err)
	}
	if err := encoder.Close(); err != nil {
		return 0, fmt.Errorf("finish encoding %s: %w", packageFile, err)
	}
	if err := writeConnectDeliveryOneRewrite(packageFile, info.Mode().Perm(), encoded.Bytes()); err != nil {
		return 0, err
	}
	return len(removals), nil
}

func writeConnectDeliveryOneRewrite(path string, mode os.FileMode, raw []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".package.yaml.connect-delivery-one-*")
	if err != nil {
		return fmt.Errorf("create temporary package.yaml: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return fmt.Errorf("set temporary package.yaml mode: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return fmt.Errorf("write temporary package.yaml: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary package.yaml: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace package.yaml: %w", err)
	}
	return nil
}

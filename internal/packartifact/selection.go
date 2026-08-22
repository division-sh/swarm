package packartifact

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	PackSelectionRelativePath = ".swarm/pack-selection.yaml"
	PackSelectionVersion      = 1
)

type PackSelectionReceipt struct {
	Version         int           `yaml:"version" json:"version"`
	BaseMode        SelectionMode `yaml:"base_mode" json:"base_mode"`
	BaseDigest      string        `yaml:"base_digest" json:"base_digest"`
	EffectiveDigest string        `yaml:"effective_digest" json:"effective_digest"`
}

func (i *EffectivePackInventory) SelectionReceipt() PackSelectionReceipt {
	if i == nil {
		return PackSelectionReceipt{}
	}
	return PackSelectionReceipt{
		Version: PackSelectionVersion, BaseMode: i.BaseSelectionMode(),
		BaseDigest: i.BaseDigest(), EffectiveDigest: i.Digest(),
	}
}

func (i *EffectivePackInventory) SelectionReceiptBody() ([]byte, error) {
	receipt := i.SelectionReceipt()
	if err := receipt.Validate(); err != nil {
		return nil, err
	}
	body, err := yaml.Marshal(receipt)
	if err != nil {
		return nil, fmt.Errorf("marshal pack selection receipt: %w", err)
	}
	return body, nil
}

func ParsePackSelectionReceipt(body []byte) (PackSelectionReceipt, error) {
	var receipt PackSelectionReceipt
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(&receipt); err != nil {
		return PackSelectionReceipt{}, fmt.Errorf("parse pack selection receipt: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return PackSelectionReceipt{}, fmt.Errorf("pack selection receipt contains multiple YAML documents")
		}
		return PackSelectionReceipt{}, fmt.Errorf("parse pack selection receipt trailing document: %w", err)
	}
	if err := receipt.Validate(); err != nil {
		return PackSelectionReceipt{}, err
	}
	return receipt, nil
}

func (r PackSelectionReceipt) Validate() error {
	if r.Version != PackSelectionVersion {
		return fmt.Errorf("pack selection receipt version %d is unsupported", r.Version)
	}
	if !r.BaseMode.Valid() {
		return fmt.Errorf("pack selection receipt base_mode %q is invalid", r.BaseMode)
	}
	if !strings.HasPrefix(strings.TrimSpace(r.BaseDigest), "sha256:") || len(strings.TrimSpace(r.BaseDigest)) != len("sha256:")+64 {
		return fmt.Errorf("pack selection receipt base_digest is invalid")
	}
	if !strings.HasPrefix(strings.TrimSpace(r.EffectiveDigest), "sha256:") || len(strings.TrimSpace(r.EffectiveDigest)) != len("sha256:")+64 {
		return fmt.Errorf("pack selection receipt effective_digest is invalid")
	}
	return nil
}

func (r PackSelectionReceipt) MatchesBase(base *PlatformPackInventory) bool {
	return base != nil && r.BaseMode == base.SelectionMode() && strings.TrimSpace(r.BaseDigest) == base.Digest()
}

func (r PackSelectionReceipt) Matches(base *PlatformPackInventory, effective *EffectivePackInventory) bool {
	return r.MatchesBase(base) && effective != nil && strings.TrimSpace(r.EffectiveDigest) == effective.Digest()
}

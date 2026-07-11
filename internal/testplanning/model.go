package testplanning

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
)

const WeightModelVersion = 1

type WeightModel struct {
	Version     int                `json:"version"`
	SourceRunID string             `json:"source_run_id"`
	Packages    map[string]float64 `json:"packages"`
}

func LoadWeightModel(r io.Reader) (WeightModel, error) {
	if r == nil {
		return WeightModel{}, fmt.Errorf("weight model reader is nil")
	}
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	var model WeightModel
	if err := decoder.Decode(&model); err != nil {
		return WeightModel{}, fmt.Errorf("decode weight model: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return WeightModel{}, fmt.Errorf("decode weight model: trailing JSON")
		}
		return WeightModel{}, fmt.Errorf("decode weight model trailing data: %w", err)
	}
	if err := model.Validate(); err != nil {
		return WeightModel{}, err
	}
	return model, nil
}

func (m WeightModel) Validate() error {
	if m.Version != WeightModelVersion {
		return fmt.Errorf("weight model version = %d, want %d", m.Version, WeightModelVersion)
	}
	if strings.TrimSpace(m.SourceRunID) == "" {
		return fmt.Errorf("weight model source_run_id must be non-empty")
	}
	for pkg, weight := range m.Packages {
		if strings.TrimSpace(pkg) == "" {
			return fmt.Errorf("weight model contains an empty package")
		}
		if math.IsNaN(weight) || math.IsInf(weight, 0) || weight < 0 {
			return fmt.Errorf("weight for %s must be finite and non-negative", pkg)
		}
	}
	return nil
}

func (m WeightModel) SortedPackages() []string {
	out := make([]string, 0, len(m.Packages))
	for pkg := range m.Packages {
		out = append(out, pkg)
	}
	sort.Strings(out)
	return out
}

func WriteWeightModel(w io.Writer, model WeightModel) error {
	if err := model.Validate(); err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(model); err != nil {
		return fmt.Errorf("encode weight model: %w", err)
	}
	return nil
}

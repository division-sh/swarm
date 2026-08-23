package testcatalog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
	"gopkg.in/yaml.v3"
)

type Disposition string

const (
	DispositionRuntime    Disposition = "runtime"
	DispositionVerifyOnly Disposition = "verify-only"
	DispositionRetired    Disposition = "retired"
)

type VerifyResult string

const (
	VerifyPass    VerifyResult = "pass"
	VerifyWarning VerifyResult = "warning"
	VerifyReject  VerifyResult = "reject"
)

type Diagnostic struct {
	Category string `yaml:"category"`
	Contains string `yaml:"contains"`
}

type Retirement struct {
	Reason      string `yaml:"reason"`
	Replacement string `yaml:"replacement"`
}

type PublicCompanion struct {
	Path      string `yaml:"path"`
	ProofRole string `yaml:"proof_role"`
}

type Metadata struct {
	Disposition     Disposition      `yaml:"disposition"`
	Verify          VerifyResult     `yaml:"verify"`
	Proves          []string         `yaml:"proves"`
	Diagnostic      *Diagnostic      `yaml:"diagnostic,omitempty"`
	Retirement      *Retirement      `yaml:"retirement,omitempty"`
	PublicCompanion *PublicCompanion `yaml:"public_companion,omitempty"`
}

type Claim struct {
	ID                  string
	Status              string      `yaml:"status"`
	RequiredDisposition Disposition `yaml:"required_disposition"`
	Scope               string      `yaml:"scope"`
}

type Fixture struct {
	Tier         string
	Name         string
	RelativePath string
	Root         string
	ExpectedPath string
	Metadata     Metadata
}

type ExternalProof struct {
	Source   string   `yaml:"source"`
	Executor string   `yaml:"executor"`
	Proves   []string `yaml:"proves"`
}

func (f Fixture) HasClaim(claimID string) bool {
	for _, candidate := range f.Metadata.Proves {
		if candidate == claimID {
			return true
		}
	}
	return false
}

type Inventory struct {
	Fixtures       []Fixture
	Claims         map[string]Claim
	ExternalProofs []ExternalProof
}

func Load(repoRoot string) (*Inventory, error) {
	repoRoot = filepath.Clean(strings.TrimSpace(repoRoot))
	if repoRoot == "." || repoRoot == "" {
		return nil, fmt.Errorf("catalog inventory requires a repository root")
	}
	claims, externalProofs, err := loadCatalogContract(filepath.Join(repoRoot, "platform-spec.yaml"))
	if err != nil {
		return nil, err
	}
	fixtures, err := discoverFixtures(repoRoot)
	if err != nil {
		return nil, err
	}
	if err := validateCoverage(repoRoot, fixtures, claims, externalProofs); err != nil {
		return nil, err
	}
	return &Inventory{Fixtures: fixtures, Claims: claims, ExternalProofs: externalProofs}, nil
}

func (i *Inventory) Select(claimID string, disposition Disposition) []Fixture {
	if i == nil {
		return nil
	}
	out := make([]Fixture, 0)
	for _, fixture := range i.Fixtures {
		if fixture.Metadata.Disposition == disposition && fixture.HasClaim(claimID) {
			out = append(out, fixture)
		}
	}
	return out
}

func (i *Inventory) PublicCompanions() []Fixture {
	if i == nil {
		return nil
	}
	out := make([]Fixture, 0)
	for _, fixture := range i.Fixtures {
		if fixture.Metadata.PublicCompanion != nil {
			out = append(out, fixture)
		}
	}
	return out
}

func discoverFixtures(repoRoot string) ([]Fixture, error) {
	testsRoot := filepath.Join(repoRoot, "tests")
	tiers, err := os.ReadDir(testsRoot)
	if err != nil {
		return nil, fmt.Errorf("read catalog root: %w", err)
	}
	fixtures := make([]Fixture, 0)
	for _, tier := range tiers {
		if !tier.IsDir() || !strings.HasPrefix(tier.Name(), "tier") {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(testsRoot, tier.Name()))
		if err != nil {
			return nil, fmt.Errorf("read catalog tier %s: %w", tier.Name(), err)
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			root := filepath.Join(testsRoot, tier.Name(), entry.Name())
			expectedPath := filepath.Join(root, "expected.yaml")
			metadata, err := loadMetadata(expectedPath)
			if err != nil {
				return nil, fmt.Errorf("catalog fixture %s/%s: %w", tier.Name(), entry.Name(), err)
			}
			fixture := Fixture{
				Tier: tier.Name(), Name: entry.Name(),
				RelativePath: filepath.ToSlash(filepath.Join("tests", tier.Name(), entry.Name())),
				Root:         root, ExpectedPath: expectedPath, Metadata: metadata,
			}
			if err := validateFixture(fixture); err != nil {
				return nil, err
			}
			fixtures = append(fixtures, fixture)
		}
	}
	sort.Slice(fixtures, func(a, b int) bool { return fixtures[a].RelativePath < fixtures[b].RelativePath })
	return fixtures, nil
}

func loadMetadata(path string) (Metadata, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Metadata{}, fmt.Errorf("read expected.yaml: %w", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return Metadata{}, fmt.Errorf("parse expected.yaml: %w", err)
	}
	root, err := documentMapping(&doc)
	if err != nil {
		return Metadata{}, err
	}
	conformance, err := uniqueMappingValue(root, "conformance")
	if err != nil {
		return Metadata{}, err
	}
	if conformance == nil {
		return Metadata{}, fmt.Errorf("missing top-level conformance metadata")
	}
	encoded, err := yaml.Marshal(conformance)
	if err != nil {
		return Metadata{}, fmt.Errorf("encode conformance metadata: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	var metadata Metadata
	if err := decoder.Decode(&metadata); err != nil {
		return Metadata{}, fmt.Errorf("decode conformance metadata: %w", err)
	}
	return metadata, nil
}

func validateFixture(fixture Fixture) error {
	metadata := fixture.Metadata
	if len(metadata.Proves) == 0 {
		return fmt.Errorf("catalog fixture %s proves no canonical claim", fixture.RelativePath)
	}
	seen := map[string]struct{}{}
	for index, raw := range metadata.Proves {
		claimID := strings.TrimSpace(raw)
		if claimID == "" || claimID != raw {
			return fmt.Errorf("catalog fixture %s has invalid claim at index %d", fixture.RelativePath, index)
		}
		if _, duplicate := seen[claimID]; duplicate {
			return fmt.Errorf("catalog fixture %s repeats claim %q", fixture.RelativePath, claimID)
		}
		seen[claimID] = struct{}{}
	}

	switch metadata.Disposition {
	case DispositionRuntime:
		if metadata.Verify != VerifyPass || metadata.Diagnostic != nil || metadata.Retirement != nil {
			return fmt.Errorf("catalog fixture %s runtime metadata must declare verify: pass only", fixture.RelativePath)
		}
	case DispositionVerifyOnly:
		if metadata.Retirement != nil {
			return fmt.Errorf("catalog fixture %s verify-only metadata cannot declare retirement", fixture.RelativePath)
		}
		switch metadata.Verify {
		case VerifyPass:
			if metadata.Diagnostic != nil {
				return fmt.Errorf("catalog fixture %s verify pass cannot declare a diagnostic", fixture.RelativePath)
			}
		case VerifyWarning, VerifyReject:
			if metadata.Diagnostic == nil || strings.TrimSpace(metadata.Diagnostic.Category) == "" || strings.TrimSpace(metadata.Diagnostic.Contains) == "" {
				return fmt.Errorf("catalog fixture %s %s requires exact diagnostic category and teaching evidence", fixture.RelativePath, metadata.Verify)
			}
		default:
			return fmt.Errorf("catalog fixture %s has invalid verify result %q", fixture.RelativePath, metadata.Verify)
		}
	case DispositionRetired:
		if metadata.Verify != "" || metadata.Diagnostic != nil || metadata.Retirement == nil ||
			strings.TrimSpace(metadata.Retirement.Reason) == "" || strings.TrimSpace(metadata.Retirement.Replacement) == "" {
			return fmt.Errorf("catalog fixture %s retired metadata requires reason and replacement and no verification claim", fixture.RelativePath)
		}
	default:
		return fmt.Errorf("catalog fixture %s has invalid disposition %q", fixture.RelativePath, metadata.Disposition)
	}

	companionPath := filepath.Join(fixture.Root, "tests", "visible-smoke.yaml")
	_, statErr := os.Stat(companionPath)
	hasCompanion := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return fmt.Errorf("catalog fixture %s stat public companion: %w", fixture.RelativePath, statErr)
	}
	if hasCompanion != (metadata.PublicCompanion != nil) {
		return fmt.Errorf("catalog fixture %s public companion metadata disagrees with filesystem presence", fixture.RelativePath)
	}
	if metadata.PublicCompanion != nil && (metadata.PublicCompanion.Path != "tests/visible-smoke.yaml" || metadata.PublicCompanion.ProofRole != "protocol-only") {
		return fmt.Errorf("catalog fixture %s public companion must be tests/visible-smoke.yaml with protocol-only proof role", fixture.RelativePath)
	}
	return nil
}

func loadCatalogContract(path string) (map[string]Claim, []ExternalProof, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read platform spec: %w", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, nil, fmt.Errorf("parse platform spec: %w", err)
	}
	root, err := documentMapping(&doc)
	if err != nil {
		return nil, nil, fmt.Errorf("platform spec: %w", err)
	}
	testSpecification, err := requiredMapping(root, "test_specification")
	if err != nil {
		return nil, nil, err
	}
	conformance, err := requiredMapping(testSpecification, "internal_catalog_conformance")
	if err != nil {
		return nil, nil, err
	}
	claimsNode, err := requiredMapping(conformance, "claims")
	if err != nil {
		return nil, nil, err
	}
	claims := make(map[string]Claim, len(claimsNode.Content)/2)
	for index := 0; index+1 < len(claimsNode.Content); index += 2 {
		claimID := strings.TrimSpace(claimsNode.Content[index].Value)
		if claimID == "" {
			return nil, nil, fmt.Errorf("platform spec catalog claim has empty id")
		}
		if _, duplicate := claims[claimID]; duplicate {
			return nil, nil, fmt.Errorf("platform spec repeats catalog claim %q", claimID)
		}
		encoded, err := yaml.Marshal(claimsNode.Content[index+1])
		if err != nil {
			return nil, nil, fmt.Errorf("encode catalog claim %s: %w", claimID, err)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(encoded))
		decoder.KnownFields(true)
		claim := Claim{ID: claimID}
		if err := decoder.Decode(&claim); err != nil {
			return nil, nil, fmt.Errorf("decode catalog claim %s: %w", claimID, err)
		}
		if claim.Status != "active" || strings.TrimSpace(claim.Scope) == "" {
			return nil, nil, fmt.Errorf("catalog claim %s must be active with a nonempty scope", claimID)
		}
		if claim.RequiredDisposition != DispositionRuntime && claim.RequiredDisposition != DispositionVerifyOnly {
			return nil, nil, fmt.Errorf("catalog claim %s has invalid required disposition %q", claimID, claim.RequiredDisposition)
		}
		claims[claimID] = claim
	}
	if len(claims) == 0 {
		return nil, nil, fmt.Errorf("platform spec declares no catalog claims")
	}
	externalNode, err := uniqueMappingValue(conformance, "external_proofs")
	if err != nil {
		return nil, nil, err
	}
	if externalNode == nil {
		return claims, nil, nil
	}
	if externalNode.Kind != yaml.SequenceNode || len(externalNode.Content) == 0 {
		return nil, nil, fmt.Errorf("platform spec external_proofs must be a nonempty sequence")
	}
	encoded, err := yaml.Marshal(externalNode)
	if err != nil {
		return nil, nil, fmt.Errorf("encode external proofs: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	var proofs []ExternalProof
	if err := decoder.Decode(&proofs); err != nil {
		return nil, nil, fmt.Errorf("decode external proofs: %w", err)
	}
	return claims, proofs, nil
}

func validateCoverage(repoRoot string, fixtures []Fixture, claims map[string]Claim, externalProofs []ExternalProof) error {
	covered := map[string]int{}
	for _, fixture := range fixtures {
		for _, claimID := range fixture.Metadata.Proves {
			claim, ok := claims[claimID]
			if !ok {
				return fmt.Errorf("catalog fixture %s references unknown claim %q", fixture.RelativePath, claimID)
			}
			if fixture.Metadata.Disposition == DispositionRetired {
				continue
			}
			if fixture.Metadata.Disposition != claim.RequiredDisposition {
				return fmt.Errorf("catalog fixture %s disposition %q cannot prove %s, which requires %q", fixture.RelativePath, fixture.Metadata.Disposition, claimID, claim.RequiredDisposition)
			}
			covered[claimID]++
		}
	}
	if err := validateExternalProofs(repoRoot, externalProofs, claims, covered); err != nil {
		return err
	}
	for _, proof := range externalProofs {
		for _, claimID := range proof.Proves {
			covered[claimID]++
		}
	}
	for claimID := range claims {
		if covered[claimID] == 0 {
			return fmt.Errorf("active catalog claim %s has no non-retired proof fixture", claimID)
		}
	}
	return nil
}

func validateExternalProofs(repoRoot string, proofs []ExternalProof, claims map[string]Claim, covered map[string]int) error {
	if len(proofs) == 0 {
		return nil
	}
	policyFile, err := os.Open(filepath.Join(repoRoot, ".github", "test-proof-plan.yaml"))
	if err != nil {
		return fmt.Errorf("external proof executor policy: %w", err)
	}
	defer policyFile.Close()
	policy, err := testplanning.LoadPolicy(policyFile)
	if err != nil {
		return fmt.Errorf("external proof executor policy: %w", err)
	}

	seenSources := map[string]struct{}{}
	seenClaims := map[string]string{}
	for index, proof := range proofs {
		if err := validateExternalProofRecord(repoRoot, policy, index, proof); err != nil {
			return err
		}
		if _, duplicate := seenSources[proof.Source]; duplicate {
			return fmt.Errorf("external proofs repeat source %q", proof.Source)
		}
		seenSources[proof.Source] = struct{}{}
		for claimIndex, claimID := range proof.Proves {
			if claimID == "" || claimID != strings.TrimSpace(claimID) {
				return fmt.Errorf("external proof %s has invalid claim at index %d", proof.Source, claimIndex)
			}
			claim, ok := claims[claimID]
			if !ok {
				return fmt.Errorf("external proof %s references unknown claim %q", proof.Source, claimID)
			}
			if claim.RequiredDisposition != DispositionRuntime {
				return fmt.Errorf("external proof %s cannot prove %s, which requires %q", proof.Source, claimID, claim.RequiredDisposition)
			}
			if owner, duplicate := seenClaims[claimID]; duplicate {
				return fmt.Errorf("external claim %s has multiple runtime-credit owners %s and %s", claimID, owner, proof.Source)
			}
			if covered[claimID] != 0 {
				return fmt.Errorf("external claim %s has multiple runtime-credit owners including tier fixtures", claimID)
			}
			seenClaims[claimID] = proof.Source
		}
	}
	return nil
}

func validateExternalProofRecord(repoRoot string, policy testplanning.Policy, index int, proof ExternalProof) error {
	if err := validateExistingContainedDirectory(repoRoot, proof.Source); err != nil {
		return fmt.Errorf("external proof source %q at index %d is an invalid repository-relative source: %w", proof.Source, index, err)
	}
	modulePrefix := strings.TrimSuffix(policy.Module, "/") + "/"
	if proof.Executor == "" || proof.Executor != strings.TrimSpace(proof.Executor) || !strings.HasPrefix(proof.Executor, modulePrefix) {
		return fmt.Errorf("external proof %s has invalid executor %q", proof.Source, proof.Executor)
	}
	executorDir := strings.TrimPrefix(proof.Executor, modulePrefix)
	if err := validateExistingContainedDirectory(repoRoot, executorDir); err != nil {
		return fmt.Errorf("external proof %s has invalid executor %q: %w", proof.Source, proof.Executor, err)
	}
	if len(proof.Proves) == 0 {
		return fmt.Errorf("external proof %s proves no canonical claim", proof.Source)
	}
	if !containsCatalogString(policy.SpecialPackages, proof.Executor) {
		return fmt.Errorf("external proof %s executor %s is not a special CI package", proof.Source, proof.Executor)
	}
	var selectedUnit string
	for _, profileName := range []string{testplanning.ProfilePRCommon, testplanning.ProfilePREscalated, testplanning.ProfileFull, testplanning.ProfileNightly} {
		profile := policy.Profiles[profileName]
		owners := make([]string, 0, 1)
		for _, unitID := range profile.Units {
			if containsCatalogString(policy.Units[unitID].Packages, proof.Executor) {
				owners = append(owners, unitID)
			}
		}
		if len(owners) != 1 {
			return fmt.Errorf("external proof %s executor %s has %d CI owners in profile %s, want exactly one", proof.Source, proof.Executor, len(owners), profileName)
		}
		if run := policy.Units[owners[0]].Run; run != "" {
			return fmt.Errorf("external proof %s executor %s has filtered CI owner %s in profile %s with run %q; external proofs require full-package execution", proof.Source, proof.Executor, owners[0], profileName, run)
		}
		if selectedUnit == "" {
			selectedUnit = owners[0]
		} else if selectedUnit != owners[0] {
			return fmt.Errorf("external proof %s executor %s changes CI owner from %s to %s", proof.Source, proof.Executor, selectedUnit, owners[0])
		}
	}
	return nil
}

func validateExistingContainedDirectory(repoRoot, relative string) error {
	relative = filepath.FromSlash(relative)
	if relative == "" || relative != strings.TrimSpace(relative) || filepath.IsAbs(relative) || filepath.Clean(relative) != relative {
		return fmt.Errorf("path must be a clean nonempty relative path")
	}
	root, err := filepath.Abs(repoRoot)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	candidate := filepath.Join(root, relative)
	if !pathContainedBy(root, candidate) {
		return fmt.Errorf("path escapes repository")
	}
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve repository root symlinks: %w", err)
	}
	realCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return fmt.Errorf("resolve path symlinks: %w", err)
	}
	if !pathContainedBy(realRoot, realCandidate) {
		return fmt.Errorf("resolved path escapes repository")
	}
	info, err := os.Stat(realCandidate)
	if err != nil {
		return fmt.Errorf("stat path: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory")
	}
	return nil
}

func pathContainedBy(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func containsCatalogString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func documentMapping(doc *yaml.Node) (*yaml.Node, error) {
	if doc == nil || doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("expected a YAML document mapping")
	}
	return doc.Content[0], nil
}

func uniqueMappingValue(mapping *yaml.Node, key string) (*yaml.Node, error) {
	var found *yaml.Node
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if strings.TrimSpace(mapping.Content[index].Value) != key {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("duplicate top-level %s metadata", key)
		}
		found = mapping.Content[index+1]
	}
	return found, nil
}

func requiredMapping(mapping *yaml.Node, key string) (*yaml.Node, error) {
	value, err := uniqueMappingValue(mapping, key)
	if err != nil {
		return nil, err
	}
	if value == nil || value.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("platform spec requires mapping %s", key)
	}
	return value, nil
}

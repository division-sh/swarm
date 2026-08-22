package packartifact

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packmodel"
	"gopkg.in/yaml.v3"
)

const (
	ProjectPackDirectory         = "packs"
	ProjectPackManifestFileName  = "manifest.yaml"
	ProjectPackManifestVersion   = 1
	ProjectPackManifestLabel     = "packs/manifest.yaml"
	ProjectPackSelectionFileName = ".selection.yaml"
)

type ProjectPackManifest struct {
	Version int                         `yaml:"version"`
	Imports []ProjectPackManifestImport `yaml:"imports"`
}

type ProjectPackManifestImport struct {
	ID     string       `yaml:"id"`
	Type   string       `yaml:"type"`
	Path   string       `yaml:"path"`
	Origin ImportOrigin `yaml:"origin"`
}

type ProjectPackSet struct {
	ManifestPath string
	ManifestBody []byte
	Sources      []ProjectPackSource
	Files        []ProjectPackFile
}

type ProjectPackFile struct {
	RelativePath string
	AbsolutePath string
	Body         []byte
}

func LoadProjectPackSet(projectRoot string) (ProjectPackSet, error) {
	root, present, err := resolveProjectPackRoot(projectRoot)
	if err != nil {
		return ProjectPackSet{}, err
	}
	if !present {
		return ProjectPackSet{}, nil
	}
	packsRoot := filepath.Join(root, ProjectPackDirectory)
	if _, err := os.Lstat(packsRoot); err != nil {
		if os.IsNotExist(err) {
			return ProjectPackSet{}, nil
		}
		return ProjectPackSet{}, fmt.Errorf("inspect project pack directory: %w", err)
	}
	transaction, err := acquireProjectPackTransaction(root)
	if err != nil {
		return ProjectPackSet{}, err
	}
	set, loadErr := loadProjectPackSetLocked(root)
	closeErr := transaction.close()
	if loadErr != nil {
		return ProjectPackSet{}, loadErr
	}
	if closeErr != nil {
		return ProjectPackSet{}, closeErr
	}
	return set, nil
}

func loadProjectPackSetLocked(root string) (ProjectPackSet, error) {
	packsRoot := filepath.Join(root, ProjectPackDirectory)
	manifestPath := filepath.Join(packsRoot, ProjectPackManifestFileName)
	rootInfo, err := os.Lstat(packsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return ProjectPackSet{}, nil
		}
		return ProjectPackSet{}, fmt.Errorf("inspect project pack directory: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return ProjectPackSet{}, fmt.Errorf("project pack path %q must be a real directory", ProjectPackDirectory)
	}
	manifestInfo, err := os.Lstat(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ProjectPackSet{}, fmt.Errorf("project pack directory exists but %s is missing", ProjectPackManifestLabel)
		}
		return ProjectPackSet{}, fmt.Errorf("inspect project pack manifest: %w", err)
	}
	if manifestInfo.Mode()&os.ModeSymlink != 0 || !manifestInfo.Mode().IsRegular() {
		return ProjectPackSet{}, fmt.Errorf("project pack manifest %s must be a regular file", ProjectPackManifestLabel)
	}
	manifestBody, err := os.ReadFile(manifestPath)
	if err != nil {
		return ProjectPackSet{}, fmt.Errorf("read project pack manifest: %w", err)
	}
	manifest, err := ParseProjectPackManifest(manifestBody)
	if err != nil {
		return ProjectPackSet{}, err
	}

	set := ProjectPackSet{
		ManifestPath: manifestPath,
		ManifestBody: append([]byte(nil), manifestBody...),
		Files: []ProjectPackFile{{
			RelativePath: ProjectPackManifestLabel,
			AbsolutePath: manifestPath,
			Body:         append([]byte(nil), manifestBody...),
		}},
	}
	expected := map[string]struct{}{ProjectPackManifestLabel: {}}
	seenIDs := make(map[string]string, len(manifest.Imports))
	seenPaths := make(map[string]string, len(manifest.Imports))
	for index, declared := range manifest.Imports {
		declared.ID = strings.TrimSpace(declared.ID)
		declared.Type = strings.TrimSpace(declared.Type)
		declared.Path = cleanRelativePath(declared.Path)
		if declared.ID == "" || declared.Path == "" || packmodel.ManifestFileNameForType(declared.Type) == "" {
			return ProjectPackSet{}, fmt.Errorf("project pack imports[%d] requires valid id, type, and canonical relative path", index)
		}
		if previous, duplicate := seenIDs[declared.ID]; duplicate {
			return ProjectPackSet{}, fmt.Errorf("duplicate project pack id %q at %q and %q", declared.ID, previous, declared.Path)
		}
		if previous, duplicate := seenPaths[declared.Path]; duplicate {
			return ProjectPackSet{}, fmt.Errorf("project pack path %q is shared by %q and %q", declared.Path, previous, declared.ID)
		}
		if !declared.Origin.Valid() {
			return ProjectPackSet{}, fmt.Errorf("project pack %q import origin is invalid", declared.ID)
		}
		seenIDs[declared.ID] = declared.Path
		seenPaths[declared.Path] = declared.ID

		directoryRelative := path.Join(ProjectPackDirectory, declared.Path)
		directoryAbsolute := filepath.Join(packsRoot, filepath.FromSlash(declared.Path))
		if err := requireRealDirectoryWithin(packsRoot, directoryAbsolute, directoryRelative); err != nil {
			return ProjectPackSet{}, err
		}
		envelopeRelative := path.Join(directoryRelative, EnvelopeFileName)
		manifestRelative := path.Join(directoryRelative, packmodel.ManifestFileNameForType(declared.Type))
		envelopeBody, envelopePath, err := readProjectPackRegularFile(root, envelopeRelative)
		if err != nil {
			return ProjectPackSet{}, err
		}
		body, bodyPath, err := readProjectPackRegularFile(root, manifestRelative)
		if err != nil {
			return ProjectPackSet{}, err
		}
		envelope, err := packmodel.ParseEnvelope(envelopeBody)
		if err != nil {
			return ProjectPackSet{}, fmt.Errorf("parse project pack %q envelope: %w", declared.ID, err)
		}
		if envelope.ID != declared.ID || envelope.Type != declared.Type {
			return ProjectPackSet{}, fmt.Errorf("project pack manifest declares %q type %q but envelope owns %q type %q", declared.ID, declared.Type, envelope.ID, envelope.Type)
		}
		if envelope.Provenance.Source != ProvenanceProject || strings.TrimSpace(envelope.ManifestHash) != ManifestHashDerived {
			return ProjectPackSet{}, fmt.Errorf("project pack %q must declare provenance.source %q and manifest_hash %q", declared.ID, ProvenanceProject, ManifestHashDerived)
		}
		if envelope.ID != declared.Origin.ID {
			return ProjectPackSet{}, fmt.Errorf("project pack %q contradicts imported origin id %q", envelope.ID, declared.Origin.ID)
		}

		expected[envelopeRelative] = struct{}{}
		expected[manifestRelative] = struct{}{}
		set.Sources = append(set.Sources, ProjectPackSource{
			Path:         directoryRelative,
			EnvelopeBody: append([]byte(nil), envelopeBody...),
			ManifestBody: append([]byte(nil), body...),
			Origin:       declared.Origin,
		})
		set.Files = append(set.Files,
			ProjectPackFile{RelativePath: envelopeRelative, AbsolutePath: envelopePath, Body: append([]byte(nil), envelopeBody...)},
			ProjectPackFile{RelativePath: manifestRelative, AbsolutePath: bodyPath, Body: append([]byte(nil), body...)},
		)
	}
	if err := rejectUnexpectedProjectPackEntries(packsRoot, expected); err != nil {
		return ProjectPackSet{}, err
	}
	sort.Slice(set.Sources, func(i, j int) bool { return set.Sources[i].Path < set.Sources[j].Path })
	sort.Slice(set.Files, func(i, j int) bool { return set.Files[i].RelativePath < set.Files[j].RelativePath })
	return set, nil
}

func resolveProjectPackRoot(projectRoot string) (string, bool, error) {
	projectRoot = strings.TrimSpace(projectRoot)
	if projectRoot == "" {
		return "", false, nil
	}
	root, err := filepath.Abs(projectRoot)
	if err != nil {
		return "", false, fmt.Errorf("resolve project pack root: %w", err)
	}
	return root, true, nil
}

func ParseProjectPackManifest(body []byte) (ProjectPackManifest, error) {
	var manifest ProjectPackManifest
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return ProjectPackManifest{}, fmt.Errorf("parse project pack manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return ProjectPackManifest{}, fmt.Errorf("project pack manifest contains multiple YAML documents")
		}
		return ProjectPackManifest{}, fmt.Errorf("parse project pack manifest trailing document: %w", err)
	}
	if manifest.Version != ProjectPackManifestVersion {
		return ProjectPackManifest{}, fmt.Errorf("project pack manifest version %d is unsupported", manifest.Version)
	}
	if len(manifest.Imports) == 0 {
		return ProjectPackManifest{}, fmt.Errorf("project pack manifest imports are required")
	}
	return manifest, nil
}

func ImportEmbeddedPack(projectRoot, id string, embedded *PlatformPackInventory) (bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return false, fmt.Errorf("pack id is required")
	}
	if embedded == nil || embedded.SelectionMode() != SelectionEmbedded {
		return false, fmt.Errorf("embedded platform pack inventory is required")
	}
	entry, ok := embedded.Lookup(id)
	if !ok {
		available := make([]string, 0, len(embedded.Entries()))
		for _, candidate := range embedded.Entries() {
			available = append(available, candidate.ID())
		}
		return false, fmt.Errorf("embedded pack %q is unavailable; available embedded packs: %s", id, strings.Join(available, ", "))
	}
	root, present, err := resolveProjectPackRoot(projectRoot)
	if err != nil || !present {
		return false, fmt.Errorf("selected project root is required")
	}
	if info, statErr := os.Stat(filepath.Join(root, "package.yaml")); statErr != nil || !info.Mode().IsRegular() {
		return false, fmt.Errorf("selected project %q has no package.yaml", root)
	}
	transaction, err := acquireProjectPackTransaction(root)
	if err != nil {
		return false, err
	}
	changed, importErr := importEmbeddedPackLocked(root, id, entry, transaction)
	closeErr := transaction.close()
	if importErr != nil {
		return false, importErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	return changed, nil
}

func importEmbeddedPackLocked(root, id string, entry Entry, transaction *projectPackTransaction) (bool, error) {
	packsRoot := filepath.Join(root, ProjectPackDirectory)
	if info, statErr := os.Lstat(packsRoot); statErr == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return false, fmt.Errorf("project pack path %q must be a real directory", ProjectPackDirectory)
	} else if statErr != nil && !os.IsNotExist(statErr) {
		return false, fmt.Errorf("inspect project pack directory: %w", statErr)
	}

	envelope := entry.Envelope()
	envelope.Provenance.Source = ProvenanceProject
	envelope.ManifestHash = ManifestHashDerived
	envelopeBody, err := yaml.Marshal(envelope)
	if err != nil {
		return false, fmt.Errorf("marshal project pack %q envelope: %w", id, err)
	}
	manifestFile := packmodel.ManifestFileNameForType(entry.Type())
	declared := ProjectPackManifestImport{
		ID: id, Type: entry.Type(), Path: id,
		Origin: ImportOrigin{Source: ProvenanceEmbedded, ID: entry.ID(), Version: entry.Version(), ManifestHash: entry.ManifestHash()},
	}
	expectedManifest := ProjectPackManifest{Version: ProjectPackManifestVersion, Imports: []ProjectPackManifestImport{declared}}
	manifestPath := filepath.Join(packsRoot, ProjectPackManifestFileName)
	set, loadErr := loadProjectPackSetLocked(root)
	if loadErr != nil {
		return false, loadErr
	}
	if len(set.ManifestBody) > 0 {
		manifest, parseErr := ParseProjectPackManifest(set.ManifestBody)
		if parseErr != nil {
			return false, parseErr
		}
		for _, existing := range manifest.Imports {
			if strings.TrimSpace(existing.ID) != id && cleanRelativePath(existing.Path) != id {
				expectedManifest.Imports = append(expectedManifest.Imports, existing)
				continue
			}
			if projectImportEqual(existing, declared) && projectPackSourceEqual(set.Sources, id, envelopeBody, entry.ManifestBody()) {
				return false, nil
			}
			return false, fmt.Errorf("project pack %q already exists with different membership or edited bytes; import will not overwrite it", id)
		}
	}
	packPath := filepath.Join(packsRoot, id)
	if _, statErr := os.Lstat(packPath); statErr == nil {
		return false, fmt.Errorf("project pack path %q already exists and import will not overwrite it", path.Join(ProjectPackDirectory, id))
	} else if !os.IsNotExist(statErr) {
		return false, fmt.Errorf("inspect project pack path %q: %w", id, statErr)
	}
	sort.Slice(expectedManifest.Imports, func(i, j int) bool { return expectedManifest.Imports[i].ID < expectedManifest.Imports[j].ID })
	manifestBody, err := yaml.Marshal(expectedManifest)
	if err != nil {
		return false, fmt.Errorf("marshal project pack manifest: %w", err)
	}
	packsRootExisted := true
	if _, statErr := os.Lstat(packsRoot); os.IsNotExist(statErr) {
		packsRootExisted = false
	} else if statErr != nil {
		return false, fmt.Errorf("inspect project packs directory: %w", statErr)
	}
	if !packsRootExisted {
		if err := os.Mkdir(packsRoot, 0o755); err != nil {
			return false, fmt.Errorf("create project packs directory: %w", err)
		}
	}
	cleanupEmptyRoot := func() {
		if !packsRootExisted {
			_ = os.Remove(packsRoot)
		}
	}
	if transaction == nil || strings.TrimSpace(transaction.stateRoot) == "" {
		cleanupEmptyRoot()
		return false, fmt.Errorf("project pack transaction is required")
	}
	tmp, err := os.MkdirTemp(transaction.stateRoot, ".pack-import-*")
	if err != nil {
		cleanupEmptyRoot()
		return false, fmt.Errorf("stage project pack import: %w", err)
	}
	defer os.RemoveAll(tmp)
	if err := os.WriteFile(filepath.Join(tmp, EnvelopeFileName), envelopeBody, 0o644); err != nil {
		cleanupEmptyRoot()
		return false, err
	}
	if err := os.WriteFile(filepath.Join(tmp, manifestFile), entry.ManifestBody(), 0o644); err != nil {
		cleanupEmptyRoot()
		return false, err
	}
	if err := os.Rename(tmp, packPath); err != nil {
		cleanupEmptyRoot()
		return false, fmt.Errorf("publish project pack %q: %w", id, err)
	}
	manifestTmp, err := os.CreateTemp(transaction.stateRoot, ".pack-manifest-*")
	if err != nil {
		_ = os.RemoveAll(packPath)
		cleanupEmptyRoot()
		return false, err
	}
	manifestTmpPath := manifestTmp.Name()
	defer os.Remove(manifestTmpPath)
	if _, err := manifestTmp.Write(manifestBody); err != nil {
		_ = manifestTmp.Close()
		_ = os.RemoveAll(packPath)
		cleanupEmptyRoot()
		return false, err
	}
	if err := manifestTmp.Close(); err != nil {
		_ = os.RemoveAll(packPath)
		cleanupEmptyRoot()
		return false, err
	}
	if err := os.Rename(manifestTmpPath, manifestPath); err != nil {
		_ = os.RemoveAll(packPath)
		cleanupEmptyRoot()
		return false, fmt.Errorf("publish project pack manifest: %w", err)
	}
	return true, nil
}

func projectImportEqual(a, b ProjectPackManifestImport) bool {
	return strings.TrimSpace(a.ID) == strings.TrimSpace(b.ID) && strings.TrimSpace(a.Type) == strings.TrimSpace(b.Type) &&
		cleanRelativePath(a.Path) == cleanRelativePath(b.Path) && a.Origin == b.Origin
}

func projectPackSourceEqual(sources []ProjectPackSource, id string, envelopeBody, manifestBody []byte) bool {
	for _, source := range sources {
		envelope, err := packmodel.ParseEnvelope(source.EnvelopeBody)
		if err == nil && envelope.ID == id {
			return bytes.Equal(source.EnvelopeBody, envelopeBody) && bytes.Equal(source.ManifestBody, manifestBody)
		}
	}
	return false
}

func requireRealDirectoryWithin(root, target, display string) error {
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return fmt.Errorf("project pack path %q escapes the packs root", display)
	}
	current := root
	for _, segment := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, segment)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			return fmt.Errorf("inspect project pack path %q: %w", display, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("project pack path %q must contain only real directories", display)
		}
	}
	return nil
}

func readProjectPackRegularFile(projectRoot, relative string) ([]byte, string, error) {
	absolute := filepath.Join(projectRoot, filepath.FromSlash(relative))
	info, err := os.Lstat(absolute)
	if err != nil {
		return nil, "", fmt.Errorf("read project pack file %q: %w", relative, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, "", fmt.Errorf("project pack file %q must be a regular file", relative)
	}
	body, err := os.ReadFile(absolute)
	if err != nil {
		return nil, "", fmt.Errorf("read project pack file %q: %w", relative, err)
	}
	return body, absolute, nil
}

func rejectUnexpectedProjectPackEntries(packsRoot string, expected map[string]struct{}) error {
	expectedDirectories := map[string]struct{}{ProjectPackDirectory: {}}
	for name := range expected {
		for parent := path.Dir(name); parent != "." && parent != "/"; parent = path.Dir(parent) {
			expectedDirectories[parent] = struct{}{}
		}
	}
	return filepath.WalkDir(packsRoot, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(filepath.Dir(packsRoot), name)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("project pack inventory path %q is a symlink", rel)
		}
		if entry.IsDir() {
			if _, ok := expectedDirectories[rel]; !ok {
				return fmt.Errorf("project pack inventory contains unlisted directory %q", rel)
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("project pack inventory path %q is not a regular file", rel)
		}
		if _, ok := expected[rel]; !ok {
			return fmt.Errorf("project pack inventory contains unlisted file %q", rel)
		}
		return nil
	})
}

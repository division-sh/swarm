package pythonmodule

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	artifactCacheDirPerm = 0o700
	artifactTreeDirPerm  = 0o755
	artifactFilePerm     = 0o444
)

//go:generate go run ./internal/genartifactmanifest -archive testdata/python-3.13.14-wasi_sdk-24.zip -output artifact_manifest_generated.go
//go:embed testdata/python-3.13.14-wasi_sdk-24.zip
var artifactFS embed.FS

var (
	artifactOnce sync.Once
	artifactDir  string
	artifactErr  error

	defaultArtifactCacheRoot, defaultArtifactCacheRootErr = resolveDefaultArtifactCacheBaseDir()
	artifactCacheBaseDir                                  = defaultArtifactCacheBaseDir
)

type artifactManifest struct {
	directories []string
	files       []artifactManifestFile
}

type artifactManifestFile struct {
	path   string
	size   int64
	digest [sha256.Size]byte
}

func materializedArtifactDir() (string, error) {
	artifactOnce.Do(func() {
		cacheRoot, err := artifactCacheBaseDir()
		if err != nil {
			artifactErr = fmt.Errorf("resolve CPython-WASI artifact cache: %w", err)
			return
		}
		artifactDir, artifactErr = materializeEmbeddedArtifact(cacheRoot)
	})
	return artifactDir, artifactErr
}

func defaultArtifactCacheBaseDir() (string, error) {
	return defaultArtifactCacheRoot, defaultArtifactCacheRootErr
}

func resolveDefaultArtifactCacheBaseDir() (string, error) {
	root, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(root) == "" {
		return "", fmt.Errorf("user cache directory is empty")
	}
	return filepath.Join(root, "swarm", "runtime-artifacts", "python"), nil
}

func materializeEmbeddedArtifact(cacheRoot string) (string, error) {
	raw, err := artifactFS.ReadFile(artifactZipPath)
	if err != nil {
		return "", err
	}
	digestHex, err := verifyArchiveDigest(raw, InterpreterDigest)
	if err != nil {
		return "", err
	}
	return materializeVerifiedArtifact(cacheRoot, raw, digestHex, embeddedArtifactManifest)
}

func materializeArtifact(cacheRoot string, raw []byte, declaredDigest string) (string, error) {
	manifest, digestHex, err := manifestFromArchive(raw, declaredDigest)
	if err != nil {
		return "", err
	}
	return materializeVerifiedArtifact(cacheRoot, raw, digestHex, manifest)
}

func materializeVerifiedArtifact(cacheRoot string, raw []byte, digestHex string, manifest artifactManifest) (string, error) {
	cacheRoot, err := filepath.Abs(cacheRoot)
	if err != nil {
		return "", fmt.Errorf("resolve artifact cache root: %w", err)
	}
	parent := filepath.Join(cacheRoot, "sha256")
	if err := os.MkdirAll(parent, artifactCacheDirPerm); err != nil {
		return "", fmt.Errorf("create artifact cache root: %w", err)
	}
	finalDir := filepath.Join(parent, digestHex)
	if err := validateMaterializedArtifact(finalDir, manifest); err == nil {
		hasSuperseded, err := hasSupersededArtifactTrees(parent, digestHex)
		if err != nil {
			return "", fmt.Errorf("inspect superseded CPython-WASI artifact trees: %w", err)
		}
		if !hasSuperseded {
			return finalDir, nil
		}
	}

	lockParent := filepath.Join(cacheRoot, "locks", "sha256")
	if err := os.MkdirAll(lockParent, artifactCacheDirPerm); err != nil {
		return "", fmt.Errorf("create artifact lock root: %w", err)
	}
	unlock, err := lockArtifactMutation(filepath.Join(lockParent, digestHex+".lock"))
	if err != nil {
		return "", fmt.Errorf("lock CPython-WASI artifact sha256:%s: %w", digestHex, err)
	}
	defer unlock()

	if err := validateMaterializedArtifact(finalDir, manifest); err == nil {
		return acceptMaterializedArtifact(parent, digestHex, finalDir)
	}

	stagingDir, err := os.MkdirTemp(parent, "."+digestHex+".staging-")
	if err != nil {
		return "", fmt.Errorf("create artifact staging directory: %w", err)
	}
	stagingOwned := true
	defer func() {
		if stagingOwned {
			_ = removeArtifactTree(stagingDir)
		}
	}()
	if err := extractArchive(stagingDir, raw, manifest); err != nil {
		return "", err
	}
	if err := validateMaterializedArtifactShape(stagingDir, manifest); err != nil {
		return "", fmt.Errorf("validate staged CPython-WASI artifact: %w", err)
	}

	if err := validateMaterializedArtifact(finalDir, manifest); err == nil {
		return acceptMaterializedArtifact(parent, digestHex, finalDir)
	} else if !errors.Is(err, fs.ErrNotExist) {
		if err := quarantineInvalidArtifact(finalDir); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
	}
	if err := os.Rename(stagingDir, finalDir); err != nil {
		return "", fmt.Errorf("publish CPython-WASI artifact sha256:%s: %w", digestHex, err)
	}
	stagingOwned = false
	return acceptMaterializedArtifact(parent, digestHex, finalDir)
}

func acceptMaterializedArtifact(parent, digestHex, finalDir string) (string, error) {
	if err := removeSupersededArtifactTrees(parent, digestHex); err != nil {
		return "", fmt.Errorf("remove superseded CPython-WASI artifact trees: %w", err)
	}
	return finalDir, nil
}

func removeSupersededArtifactTrees(parent, digestHex string) error {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return err
	}
	prefixes := supersededArtifactTreePrefixes(digestHex)
	for _, entry := range entries {
		for _, prefix := range prefixes {
			if strings.HasPrefix(entry.Name(), prefix) {
				if err := removeArtifactTree(filepath.Join(parent, entry.Name())); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

func hasSupersededArtifactTrees(parent, digestHex string) (bool, error) {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return false, err
	}
	prefixes := supersededArtifactTreePrefixes(digestHex)
	for _, entry := range entries {
		for _, prefix := range prefixes {
			if strings.HasPrefix(entry.Name(), prefix) {
				return true, nil
			}
		}
	}
	return false, nil
}

func supersededArtifactTreePrefixes(digestHex string) []string {
	return []string{"." + digestHex + ".staging-", "." + digestHex + ".invalid-"}
}

func manifestFromArchive(raw []byte, declaredDigest string) (artifactManifest, string, error) {
	digestHex, err := verifyArchiveDigest(raw, declaredDigest)
	if err != nil {
		return artifactManifest{}, "", err
	}

	reader, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return artifactManifest{}, "", fmt.Errorf("open embedded CPython-WASI artifact: %w", err)
	}
	kinds := make(map[string]string, len(reader.File))
	directories := make(map[string]struct{})
	files := make([]artifactManifestFile, 0, len(reader.File))
	for _, file := range reader.File {
		name, err := safeArchivePath(file.Name)
		if err != nil {
			return artifactManifest{}, "", err
		}
		mode := file.Mode()
		if mode&os.ModeSymlink != 0 || (!file.FileInfo().IsDir() && !mode.IsRegular()) {
			return artifactManifest{}, "", fmt.Errorf("embedded CPython-WASI artifact contains unsupported entry %q with mode %s", file.Name, mode)
		}
		if file.FileInfo().IsDir() {
			if err := addManifestDirectory(name, kinds, directories); err != nil {
				return artifactManifest{}, "", err
			}
			continue
		}
		if _, exists := kinds[name]; exists {
			return artifactManifest{}, "", fmt.Errorf("embedded CPython-WASI artifact contains duplicate path %q", file.Name)
		}
		kinds[name] = "file"
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if err := addManifestDirectory(parent, kinds, directories); err != nil {
				return artifactManifest{}, "", err
			}
		}
		src, err := file.Open()
		if err != nil {
			return artifactManifest{}, "", fmt.Errorf("open embedded artifact entry %q: %w", file.Name, err)
		}
		hasher := sha256.New()
		size, copyErr := io.Copy(hasher, src)
		closeErr := src.Close()
		if copyErr != nil {
			return artifactManifest{}, "", fmt.Errorf("hash embedded artifact entry %q: %w", file.Name, copyErr)
		}
		if closeErr != nil {
			return artifactManifest{}, "", fmt.Errorf("close embedded artifact entry %q: %w", file.Name, closeErr)
		}
		if size != int64(file.UncompressedSize64) {
			return artifactManifest{}, "", fmt.Errorf("embedded artifact entry %q size %d does not match archive size %d", file.Name, size, file.UncompressedSize64)
		}
		var digest [sha256.Size]byte
		copy(digest[:], hasher.Sum(nil))
		files = append(files, artifactManifestFile{path: name, size: size, digest: digest})
	}
	if kinds[pythonWasmPath] != "file" {
		return artifactManifest{}, "", fmt.Errorf("embedded CPython-WASI artifact is missing %s", pythonWasmPath)
	}
	dirs := make([]string, 0, len(directories))
	for name := range directories {
		dirs = append(dirs, name)
	}
	sort.Strings(dirs)
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
	return artifactManifest{directories: dirs, files: files}, digestHex, nil
}

func verifyArchiveDigest(raw []byte, declaredDigest string) (string, error) {
	sum := sha256.Sum256(raw)
	actual := "sha256:" + hex.EncodeToString(sum[:])
	if actual != declaredDigest {
		return "", fmt.Errorf("embedded CPython-WASI artifact digest %s does not match declared %s", actual, declaredDigest)
	}
	digestHex := strings.TrimPrefix(declaredDigest, "sha256:")
	if len(digestHex) != sha256.Size*2 {
		return "", fmt.Errorf("declared CPython-WASI artifact digest %q is not canonical sha256", declaredDigest)
	}
	return digestHex, nil
}

func addManifestDirectory(name string, kinds map[string]string, directories map[string]struct{}) error {
	if kind, exists := kinds[name]; exists {
		if kind != "directory" {
			return fmt.Errorf("embedded CPython-WASI artifact path %q is both a file and directory", name)
		}
		return nil
	}
	kinds[name] = "directory"
	directories[name] = struct{}{}
	for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
		if err := addManifestDirectory(parent, kinds, directories); err != nil {
			return err
		}
	}
	return nil
}

func safeArchivePath(name string) (string, error) {
	if strings.TrimSpace(name) == "" || strings.ContainsRune(name, '\x00') || strings.Contains(name, "\\") {
		return "", fmt.Errorf("embedded CPython-WASI artifact contains unsafe path %q", name)
	}
	trimmed := strings.TrimSuffix(name, "/")
	for _, segment := range strings.Split(trimmed, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("embedded CPython-WASI artifact contains unsafe path %q", name)
		}
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) || (len(clean) >= 2 && clean[1] == ':') {
		return "", fmt.Errorf("embedded CPython-WASI artifact contains unsafe path %q", name)
	}
	local := filepath.FromSlash(clean)
	if filepath.IsAbs(local) || filepath.VolumeName(local) != "" {
		return "", fmt.Errorf("embedded CPython-WASI artifact contains unsafe path %q", name)
	}
	return clean, nil
}

func extractArchive(root string, raw []byte, manifest artifactManifest) error {
	reader, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return fmt.Errorf("open embedded CPython-WASI artifact: %w", err)
	}
	for _, directory := range manifest.directories {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(directory)), artifactTreeDirPerm); err != nil {
			return fmt.Errorf("create embedded artifact directory %q: %w", directory, err)
		}
	}
	expectedFiles := make(map[string]artifactManifestFile, len(manifest.files))
	for _, file := range manifest.files {
		expectedFiles[file.path] = file
	}
	extracted := 0
	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue
		}
		name, err := safeArchivePath(file.Name)
		if err != nil {
			return err
		}
		target := filepath.Join(root, filepath.FromSlash(name))
		src, err := file.Open()
		if err != nil {
			return fmt.Errorf("open embedded artifact entry %q: %w", file.Name, err)
		}
		dst, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			src.Close()
			return fmt.Errorf("create cached artifact entry %q: %w", name, err)
		}
		expected, ok := expectedFiles[name]
		if !ok {
			dst.Close()
			src.Close()
			return fmt.Errorf("embedded artifact entry %q is absent from verified manifest", name)
		}
		hasher := sha256.New()
		size, copyErr := io.Copy(io.MultiWriter(dst, hasher), src)
		closeErr := dst.Close()
		srcCloseErr := src.Close()
		if copyErr != nil {
			return fmt.Errorf("extract embedded artifact entry %q: %w", name, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close cached artifact entry %q: %w", name, closeErr)
		}
		if srcCloseErr != nil {
			return fmt.Errorf("close embedded artifact entry %q: %w", name, srcCloseErr)
		}
		if size != expected.size || !bytes.Equal(hasher.Sum(nil), expected.digest[:]) {
			return fmt.Errorf("extracted artifact entry %q does not match verified manifest", name)
		}
		if err := os.Chmod(target, artifactFilePerm); err != nil {
			return fmt.Errorf("make cached artifact entry %q read-only: %w", name, err)
		}
		extracted++
	}
	if extracted != len(expectedFiles) {
		return fmt.Errorf("extracted CPython-WASI artifact is incomplete: wrote %d/%d files", extracted, len(expectedFiles))
	}
	if err := os.Chmod(root, artifactTreeDirPerm); err != nil {
		return fmt.Errorf("publish artifact directory permissions: %w", err)
	}
	return nil
}

func validateMaterializedArtifact(root string, manifest artifactManifest) error {
	return validateMaterializedArtifactTree(root, manifest, true)
}

func validateMaterializedArtifactShape(root string, manifest artifactManifest) error {
	return validateMaterializedArtifactTree(root, manifest, false)
}

func validateMaterializedArtifactTree(root string, manifest artifactManifest, verifyContent bool) error {
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return fmt.Errorf("cached CPython-WASI artifact root is not a real directory")
	}
	expectedDirs := make(map[string]struct{}, len(manifest.directories))
	for _, name := range manifest.directories {
		expectedDirs[name] = struct{}{}
	}
	expectedFiles := make(map[string]artifactManifestFile, len(manifest.files))
	for _, file := range manifest.files {
		expectedFiles[file.path] = file
	}
	seenDirs := make(map[string]struct{}, len(expectedDirs))
	seenFiles := make(map[string]struct{}, len(expectedFiles))
	err = filepath.WalkDir(root, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if current == root {
			return nil
		}
		rel, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("cached CPython-WASI artifact contains symlink %q", rel)
		}
		if info.IsDir() {
			if _, ok := expectedDirs[rel]; !ok {
				return fmt.Errorf("cached CPython-WASI artifact contains extra directory %q", rel)
			}
			seenDirs[rel] = struct{}{}
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("cached CPython-WASI artifact contains non-regular file %q", rel)
		}
		expected, ok := expectedFiles[rel]
		if !ok {
			return fmt.Errorf("cached CPython-WASI artifact contains extra file %q", rel)
		}
		if info.Size() != expected.size {
			return fmt.Errorf("cached CPython-WASI artifact file %q has size %d, want %d", rel, info.Size(), expected.size)
		}
		if !verifyContent {
			seenFiles[rel] = struct{}{}
			return nil
		}
		file, err := os.Open(current)
		if err != nil {
			return err
		}
		hasher := sha256.New()
		_, hashErr := io.Copy(hasher, file)
		closeErr := file.Close()
		if hashErr != nil {
			return fmt.Errorf("hash cached CPython-WASI artifact file %q: %w", rel, hashErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close cached CPython-WASI artifact file %q: %w", rel, closeErr)
		}
		if !bytes.Equal(hasher.Sum(nil), expected.digest[:]) {
			return fmt.Errorf("cached CPython-WASI artifact file %q digest mismatch", rel)
		}
		seenFiles[rel] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}
	if len(seenDirs) != len(expectedDirs) || len(seenFiles) != len(expectedFiles) {
		return fmt.Errorf("cached CPython-WASI artifact is incomplete: found %d/%d directories and %d/%d files", len(seenDirs), len(expectedDirs), len(seenFiles), len(expectedFiles))
	}
	return nil
}

func quarantineInvalidArtifact(path string) error {
	parent := filepath.Dir(path)
	quarantine, err := os.MkdirTemp(parent, "."+filepath.Base(path)+".invalid-")
	if err != nil {
		return fmt.Errorf("reserve invalid artifact quarantine: %w", err)
	}
	if err := os.Remove(quarantine); err != nil {
		return fmt.Errorf("prepare invalid artifact quarantine: %w", err)
	}
	if err := os.Rename(path, quarantine); err != nil {
		return err
	}
	if err := removeArtifactTree(quarantine); err != nil {
		return fmt.Errorf("remove invalid cached CPython-WASI artifact: %w", err)
	}
	return nil
}

func removeArtifactTree(root string) error {
	_ = filepath.WalkDir(root, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr == nil && entry.IsDir() {
			_ = os.Chmod(current, artifactTreeDirPerm)
		}
		return nil
	})
	return os.RemoveAll(root)
}

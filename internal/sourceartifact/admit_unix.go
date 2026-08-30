//go:build linux || darwin

package sourceartifact

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

func AdmitDirectory(root string) (*AdmittedSourceArtifact, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("source root is required")
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open source root %q: %w", root, err)
	}
	defer unix.Close(fd)
	state := admissionState{entries: make([]Entry, 0)}
	if err := admitDirectoryFD(fd, "", false, &state); err != nil {
		return nil, err
	}
	return newArtifact(state.entries)
}

type admissionState struct {
	entries    []Entry
	totalBytes int
}

func (s *admissionState) requireMemberSlot() error {
	count := len(s.entries) + 1
	if count > MaxMembers {
		return fmt.Errorf("source artifact has %d members, maximum is %d", count, MaxMembers)
	}
	return nil
}

func (s *admissionState) appendEntry(label string, body []byte) error {
	if err := s.requireMemberSlot(); err != nil {
		return err
	}
	total := s.totalBytes + len(body)
	if total > MaxArtifactBytes {
		return fmt.Errorf("source artifact is %d bytes, maximum is %d", total, MaxArtifactBytes)
	}
	s.entries = append(s.entries, Entry{label: label, body: body})
	s.totalBytes = total
	return nil
}

func admitDirectoryFD(fd int, prefix string, resource bool, state *admissionState) error {
	children, err := readDirectoryNames(fd, prefix)
	if err != nil {
		return err
	}
	folded := map[string]string{}
	for _, name := range children {
		if IsExcludedDirectory(name) || IsExcludedFile(name) {
			continue
		}
		label := name
		if prefix != "" {
			label = prefix + "/" + name
		}
		isDirectory, err := inspectChildFD(fd, name, label)
		if err != nil {
			return err
		}
		fold := asciiFold(name)
		if previous, exists := folded[fold]; exists {
			return fmt.Errorf("case-colliding source entries %q and %q under %q", previous, name, prefix)
		}
		folded[fold] = name
		if resource {
			if err := ValidateLabel(label); err != nil {
				return err
			}
			if isDirectory {
				childFD, err := openChildDirectory(fd, name, label)
				if err != nil {
					return err
				}
				err = admitDirectoryFD(childFD, label, true, state)
				unix.Close(childFD)
				if err != nil {
					return err
				}
				continue
			}
			if err := state.requireMemberSlot(); err != nil {
				return err
			}
			body, err := readChildFile(fd, name, label)
			if err != nil {
				return err
			}
			if err := state.appendEntry(label, body); err != nil {
				return err
			}
			continue
		}
		if isDirectory {
			if name == "flows" {
				return fmt.Errorf("RETIRED: flows/ is not admitted; move its children directly under %q", displayFlowPrefix(prefix))
			}
			childResource := false
			if _, ok := resourceBranches[name]; ok {
				if name == "packs" && prefix != "" {
					return fmt.Errorf("packs/ is selected-root-only and cannot appear under flow %q", prefix)
				}
				childResource = true
			} else if err := ValidateFlowSegment(name); err != nil {
				return fmt.Errorf("unclassified source directory %q: %w", label, err)
			}
			childFD, err := openChildDirectory(fd, name, label)
			if err != nil {
				return err
			}
			before := len(state.entries)
			err = admitDirectoryFD(childFD, label, childResource, state)
			unix.Close(childFD)
			if err != nil {
				return err
			}
			if len(state.entries) == before && !childResource {
				return fmt.Errorf("flow %q is empty", label)
			}
			continue
		}
		if name == "package.yaml" {
			return fmt.Errorf("RETIRED: package.yaml is not admitted; use optional manifest.yaml and filesystem child flows")
		}
		if _, err := classifyLabel(label); err != nil {
			return err
		}
		if err := state.requireMemberSlot(); err != nil {
			return err
		}
		body, err := readChildFile(fd, name, label)
		if err != nil {
			return err
		}
		if err := state.appendEntry(label, body); err != nil {
			return err
		}
	}
	return nil
}

func readDirectoryNames(fd int, prefix string) ([]string, error) {
	dup, err := unix.Dup(fd)
	if err != nil {
		return nil, fmt.Errorf("duplicate source directory handle %q: %w", prefix, err)
	}
	dir := os.NewFile(uintptr(dup), prefix)
	if dir == nil {
		unix.Close(dup)
		return nil, fmt.Errorf("open source directory handle %q", prefix)
	}
	children, err := dir.Readdirnames(-1)
	_ = dir.Close()
	if err != nil {
		return nil, fmt.Errorf("enumerate source directory %q: %w", prefix, err)
	}
	sort.Strings(children)
	return children, nil
}

func inspectChildFD(parentFD int, name, label string) (bool, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(parentFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return false, fmt.Errorf("inspect source path %q: %w", label, err)
	}
	mode := stat.Mode & unix.S_IFMT
	if mode == unix.S_IFLNK {
		return false, fmt.Errorf("source path %q must not be a symlink", label)
	}
	return mode == unix.S_IFDIR, nil
}

func openChildDirectory(parentFD int, name, label string) (int, error) {
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, fmt.Errorf("open source directory %q: %w", label, err)
	}
	return fd, nil
}

func readChildFile(parentFD int, name, label string) ([]byte, error) {
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open source file %q: %w", label, err)
	}
	file := os.NewFile(uintptr(fd), label)
	if file == nil {
		unix.Close(fd)
		return nil, fmt.Errorf("open source file handle %q", label)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect source file %q: %w", label, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("source path %q must be a regular file", label)
	}
	if info.Size() > MaxMemberBytes {
		return nil, fmt.Errorf("source artifact member %q is %d bytes, maximum is %d", label, info.Size(), MaxMemberBytes)
	}
	body, err := io.ReadAll(io.LimitReader(file, MaxMemberBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read source file %q: %w", label, err)
	}
	if len(body) > MaxMemberBytes {
		return nil, fmt.Errorf("source artifact member %q exceeds %d bytes", label, MaxMemberBytes)
	}
	return body, nil
}

func displayFlowPrefix(prefix string) string {
	if prefix == "" {
		return "."
	}
	return prefix
}

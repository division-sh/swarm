//go:build linux || darwin

package packartifact

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type admittedArtifactRoot struct {
	directory *os.File
}

func openAdmittedArtifactRoot(target string) (*admittedArtifactRoot, error) {
	target = filepath.Clean(target)
	if !filepath.IsAbs(target) {
		return nil, fmt.Errorf("artifact root %q must be absolute", target)
	}
	volumeRoot := filepath.VolumeName(target) + string(filepath.Separator)
	relative, err := filepath.Rel(volumeRoot, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("artifact root %q is outside its volume", target)
	}
	directory, err := openUnixDirectoryAt(nil, volumeRoot)
	if err != nil {
		return nil, fmt.Errorf("open artifact volume root %q: %w", volumeRoot, err)
	}
	segments, err := artifactPathSegments(relative)
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	for _, segment := range segments {
		next, openErr := openUnixDirectoryAt(directory, segment)
		_ = directory.Close()
		if openErr != nil {
			return nil, fmt.Errorf("open artifact root %q component %q without following links: %w", target, segment, openErr)
		}
		directory = next
	}
	return &admittedArtifactRoot{directory: directory}, nil
}

func openUnixDirectoryAt(parent *os.File, name string) (*os.File, error) {
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_NONBLOCK
	var (
		fd  int
		err error
	)
	if parent == nil {
		fd, err = unix.Open(name, flags, 0)
	} else {
		fd, err = unix.Openat(int(parent.Fd()), name, flags, 0)
	}
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("opened directory handle is invalid")
	}
	return file, nil
}

func (r *admittedArtifactRoot) close() error {
	if r == nil || r.directory == nil {
		return nil
	}
	err := r.directory.Close()
	r.directory = nil
	return err
}

func (r *admittedArtifactRoot) openDirectory(relative string) (*os.File, error) {
	if r == nil || r.directory == nil {
		return nil, fmt.Errorf("artifact root is closed")
	}
	directory, err := openUnixDirectoryAt(r.directory, ".")
	if err != nil {
		return nil, fmt.Errorf("duplicate artifact root handle: %w", err)
	}
	segments, err := artifactPathSegments(relative)
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	for _, segment := range segments {
		next, openErr := openUnixDirectoryAt(directory, segment)
		_ = directory.Close()
		if openErr != nil {
			return nil, fmt.Errorf("open artifact directory %q without following links: %w", relative, openErr)
		}
		directory = next
	}
	return directory, nil
}

func (r *admittedArtifactRoot) readDir(relative string) ([]fs.DirEntry, error) {
	directory, err := r.openDirectory(relative)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("read artifact directory %q: %w", relative, err)
	}
	return entries, nil
}

func (r *admittedArtifactRoot) openRegularFile(relative string) (*os.File, error) {
	segments, err := artifactPathSegments(relative)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("artifact file path is required")
	}
	directory, err := r.openDirectory(filepath.Join(segments[:len(segments)-1]...))
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	name := segments[len(segments)-1]
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open artifact file %q without following links: %w", relative, err)
	}
	file := os.NewFile(uintptr(fd), relative)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open artifact file %q returned an invalid handle", relative)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect opened artifact file %q: %w", relative, err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("artifact file %q must be a regular non-symlink file", relative)
	}
	return file, nil
}

func (r *admittedArtifactRoot) readRegularFile(relative string) ([]byte, error) {
	file, err := r.openRegularFile(relative)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	body, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("read opened artifact file %q: %w", relative, err)
	}
	return body, nil
}

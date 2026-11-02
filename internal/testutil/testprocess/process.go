//go:build linux || darwin

// Package testprocess binds long-lived test helpers to their owning test process.
package testprocess

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Lifetime identifies one process occurrence, not a reusable PID.
type Lifetime struct {
	PID   int
	Birth string
}

const bootstrapEnv = "SWARM_TEST_PROCESS_BOOTSTRAP"

type bootstrap struct {
	Watch  bool
	Owner  Lifetime
	Target int
	Ready  int
	Status int
	Path   string
	Args   []string
}

// Current captures the process that owns a helper's lifetime.
func Current() (Lifetime, error) {
	return Observe(os.Getpid())
}

// Observe captures an existing process occurrence for independent termination checks.
func Observe(pid int) (Lifetime, error) {
	birth, err := processBirth(pid)
	return Lifetime{PID: pid, Birth: birth}, err
}

// Alive refuses a missing, exited, or different process occurrence.
func (owner Lifetime) Alive() bool {
	if owner.PID <= 0 || owner.Birth == "" {
		return false
	}
	birth, err := processBirth(owner.PID)
	return err == nil && birth == owner.Birth
}

// Start preserves the caller's single Wait owner and existing process attributes.
func Start(command *exec.Cmd) error {
	return startNative(command)
}

// StartFor permits a fixture to outlive an intentionally killed inner parent,
// while remaining bound to the exact outer test occurrence.
func StartFor(command *exec.Cmd, owner Lifetime) error {
	return startWatched(command, owner)
}

func init() {
	raw := os.Getenv(bootstrapEnv)
	if raw == "" {
		return
	}
	var config bootstrap
	decoded, err := base64.RawStdEncoding.DecodeString(raw)
	if err != nil || json.Unmarshal(decoded, &config) != nil {
		os.Exit(125)
	}
	if config.Watch {
		watch(config)
		os.Exit(0)
	}
	execute(config)
	os.Exit(125)
}

func bootstrapEnvironment(environment []string, config bootstrap) []string {
	clean := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if len(entry) > len(bootstrapEnv) && entry[:len(bootstrapEnv)+1] == bootstrapEnv+"=" {
			continue
		}
		clean = append(clean, entry)
	}
	raw, _ := json.Marshal(config)
	return append(clean, bootstrapEnv+"="+base64.RawStdEncoding.EncodeToString(raw))
}

func inheritedFiles(files []*os.File) ([]*os.File, func(), error) {
	// Some fixture owners intentionally pass non-CLOEXEC descriptors without
	// Cmd.ExtraFiles. Preserve their exact numbers before adding our status FD.
	directory := "/dev/fd"
	if _, err := os.Stat("/proc/self/fd"); err == nil {
		directory = "/proc/self/fd"
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, func() {}, err
	}
	result := append([]*os.File(nil), files...)
	var duplicates []*os.File
	closeFiles := func() {
		for _, file := range duplicates {
			_ = file.Close()
		}
	}
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil || fd < 3+len(files) {
			continue
		}
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if errors.Is(err, unix.EBADF) {
			continue
		}
		if err != nil {
			closeFiles()
			return nil, func() {}, err
		}
		if flags&unix.FD_CLOEXEC != 0 {
			continue
		}
		copyFD, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			closeFiles()
			return nil, func() {}, err
		}
		copy := os.NewFile(uintptr(copyFD), "test-helper-inherited")
		duplicates = append(duplicates, copy)
		for len(result) <= fd-3 {
			result = append(result, nil)
		}
		result[fd-3] = copy
	}
	return result, closeFiles, nil
}

func startWatched(command *exec.Cmd, owner Lifetime) error {
	if command == nil {
		return errors.New("test helper command is required")
	}
	if command.Err != nil {
		return command.Err
	}
	if err := validateWatched(command); err != nil {
		return err
	}
	if !owner.Alive() {
		return errors.New("test helper lifetime owner is not current")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	read, write, err := os.Pipe()
	if err != nil {
		return err
	}
	defer read.Close()
	defer write.Close()
	path, args, environment, files := command.Path, command.Args, command.Env, command.ExtraFiles
	defer func() { command.Path, command.Args, command.Env, command.ExtraFiles = path, args, environment, files }()
	forwarded, closeFiles, err := inheritedFiles(files)
	if err != nil {
		return err
	}
	defer closeFiles()
	payloadArgs := args
	if len(payloadArgs) == 0 {
		payloadArgs = []string{path}
	}
	config := bootstrap{Owner: owner, Status: 3 + len(forwarded), Path: path, Args: payloadArgs}
	command.Env = bootstrapEnvironment(command.Environ(), config)
	command.Path, command.Args = executable, []string{executable}
	command.ExtraFiles = append(forwarded, write)
	if err := command.Start(); err != nil {
		return err
	}
	write.Close()
	raw, err := io.ReadAll(read)
	if err == nil && len(raw) == 0 {
		return nil // The status descriptor closes atomically on payload exec.
	}
	_ = command.Process.Kill()
	_ = command.Wait()
	if err != nil {
		return fmt.Errorf("test helper exec handshake: %w", err)
	}
	code, err := strconv.Atoi(string(raw))
	if err != nil || code <= 0 {
		return errors.New("test helper bootstrap failed")
	}
	return &os.PathError{Op: "fork/exec", Path: path, Err: syscall.Errno(code)}
}

func execute(config bootstrap) {
	status := os.NewFile(uintptr(config.Status), "test-helper-exec-status")
	if status == nil {
		return
	}
	defer status.Close()
	syscall.CloseOnExec(config.Status)
	executable, err := os.Executable()
	if err == nil && config.Owner.Alive() {
		var read, write *os.File
		read, write, err = os.Pipe()
		if err == nil {
			defer read.Close()
			defer write.Close()
			guard := exec.Command(executable)
			guard.Env = bootstrapEnvironment(os.Environ(), bootstrap{Watch: true, Owner: config.Owner, Target: os.Getpid(), Ready: 3})
			guard.ExtraFiles = make([]*os.File, config.Status-2)
			guard.ExtraFiles[0] = write
			// Only the readiness descriptor is inherited. Payload listeners,
			// leases, stdio and exec-status cannot be retained by the guard.
			err = guard.Start()
			write.Close()
			if err == nil {
				var ready [1]byte
				_, err = io.ReadFull(read, ready[:])
				if err == nil && ready[0] == 1 && config.Owner.Alive() {
					environment := bootstrapEnvironment(os.Environ(), bootstrap{})
					environment = environment[:len(environment)-1]
					err = syscall.Exec(config.Path, config.Args, environment)
				}
			}
		}
	}
	code := syscall.ECHILD
	var errno syscall.Errno
	if errors.As(err, &errno) {
		code = errno
	}
	_, _ = fmt.Fprint(status, int(code))
}

func watch(config bootstrap) {
	ready := os.NewFile(uintptr(config.Ready), "test-helper-guard-ready")
	if ready == nil {
		return
	}
	target, err := os.FindProcess(config.Target)
	if err != nil {
		ready.Close()
		return
	}
	defer target.Release()
	if os.Getppid() != config.Target || !config.Owner.Alive() {
		ready.Close()
		return
	}
	_, _ = ready.Write([]byte{1})
	ready.Close()
	for {
		if os.Getppid() != config.Target {
			return
		}
		if !config.Owner.Alive() {
			_ = target.Kill()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

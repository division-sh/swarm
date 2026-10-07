package pythonmodule

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
	"golang.org/x/sys/cpu"
)

const compiledInterpreterFormat = "swarm-python-compiled-v1"
const compiledInterpreterMaxBytes = 128 << 20

var errInvalidCompiledInterpreter = errors.New("invalid compiled interpreter cache entry")

type interpreterPolicy struct {
	ConsumeFuel, EpochInterruption, BulkMemory, Memory64 bool
	MultiMemory, SIMD, RelaxedSIMD, Threads              bool
}

func currentInterpreterPolicy() interpreterPolicy {
	return interpreterPolicy{ConsumeFuel: true, EpochInterruption: true, BulkMemory: true}
}

type compiledInterpreterIdentity struct {
	Format, Interpreter, Snapshot, Engine, Executable, OS, Arch, CPU string
	Policy                                                           interpreterPolicy
}

var compiledExecutableOnce sync.Once
var compiledExecutableDigest string
var compiledExecutableErr error

func currentCompiledInterpreterIdentity() (compiledInterpreterIdentity, error) {
	compiledExecutableOnce.Do(func() {
		name, err := os.Executable()
		if err != nil {
			compiledExecutableErr = err
			return
		}
		file, err := os.Open(name)
		if err != nil {
			compiledExecutableErr = err
			return
		}
		digest := sha256.New()
		_, readErr := io.Copy(digest, file)
		compiledExecutableErr = errors.Join(readErr, file.Close())
		compiledExecutableDigest = hex.EncodeToString(digest.Sum(nil))
	})
	if compiledExecutableErr != nil {
		return compiledInterpreterIdentity{}, compiledExecutableErr
	}
	features, err := json.Marshal(struct {
		X86   any
		ARM64 any
	}{cpu.X86, cpu.ARM64})
	if err != nil {
		return compiledInterpreterIdentity{}, err
	}
	return compiledInterpreterIdentity{
		Format: compiledInterpreterFormat, Interpreter: InterpreterDigest,
		Snapshot: embeddedSnapshotDigest(), Engine: computemodule.EngineVersion(),
		Executable: compiledExecutableDigest,
		OS:         runtime.GOOS, Arch: runtime.GOARCH, CPU: compiledDigest(features), Policy: currentInterpreterPolicy(),
	}, nil
}

func (identity compiledInterpreterIdentity) key() string {
	raw, _ := json.Marshal(identity)
	return compiledDigest(raw)
}

func compiledDigest(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

type compiledInterpreterHeader struct {
	Identity compiledInterpreterIdentity
	Digest   string
	Size     int
}

// Only compiler-owned immutable code lives here, outside the guest-mounted tree.
// A fresh process, Engine, Store and linear memory still own every invocation.
func loadCompiledInterpreter(cacheRoot string, identity compiledInterpreterIdentity, compile func() ([]byte, error)) ([]byte, error) {
	if !filepath.IsAbs(cacheRoot) {
		return nil, fmt.Errorf("compiled interpreter requires the absolute owner cache root")
	}
	dir := filepath.Join(cacheRoot, "compiled")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.Join(fmt.Errorf("compiled interpreter requires a private real directory"), err)
	}
	name := filepath.Join(dir, identity.key()+".module")
	if raw, err := readCompiledInterpreter(name, identity); err == nil {
		return raw, nil
	} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errInvalidCompiledInterpreter) {
		return nil, err
	}
	unlock, err := lockArtifactMutation(filepath.Join(dir, identity.key()+".lock"))
	if err != nil {
		return nil, err
	}
	defer unlock()
	if raw, err := readCompiledInterpreter(name, identity); err == nil {
		return raw, nil
	} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errInvalidCompiledInterpreter) {
		return nil, err
	}
	raw, err := compile()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > compiledInterpreterMaxBytes {
		return nil, fmt.Errorf("compiled interpreter exceeds code cache bounds")
	}
	header, err := json.Marshal(compiledInterpreterHeader{Identity: identity, Digest: compiledDigest(raw), Size: len(raw)})
	if err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(dir, "."+identity.key()+".staging-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(file.Name())
	_, headerErr := file.Write(append(header, '\n'))
	_, bodyErr := file.Write(raw)
	err = errors.Join(headerErr, bodyErr, file.Sync(), file.Close())
	if err != nil {
		return nil, err
	}
	if err := os.Rename(file.Name(), name); err != nil {
		return nil, err
	}
	return raw, nil
}

func readCompiledInterpreter(name string, identity compiledInterpreterIdentity) ([]byte, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("compiled interpreter must be a private regular file")
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	opened, statErr := file.Stat()
	if statErr != nil || !os.SameFile(info, opened) || opened.Mode().Perm()&0o077 != 0 {
		return nil, errors.Join(fmt.Errorf("compiled interpreter changed while opening"), statErr, file.Close())
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, compiledInterpreterMaxBytes+8193))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, err
	}
	newline := bytes.IndexByte(raw, '\n')
	if newline < 0 || newline > 8192 {
		return nil, errInvalidCompiledInterpreter
	}
	var header compiledInterpreterHeader
	decoder := json.NewDecoder(bytes.NewReader(raw[:newline]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&header); err != nil {
		return nil, errInvalidCompiledInterpreter
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errInvalidCompiledInterpreter
	}
	body := raw[newline+1:]
	if header.Identity != identity || header.Size != len(body) || len(body) == 0 || len(body) > compiledInterpreterMaxBytes || header.Digest != compiledDigest(body) {
		return nil, errInvalidCompiledInterpreter
	}
	return body, nil
}

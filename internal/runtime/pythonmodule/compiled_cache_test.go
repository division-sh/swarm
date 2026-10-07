package pythonmodule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bytecodealliance/wasmtime-go/v46"
)

func TestCompiledInterpreterConcurrentPublishersAndWarmProcesses(t *testing.T) {
	if os.Getenv(artifactCacheHelperEnv) != "" {
		waitForArtifactCacheStartGate(t)
		root, err := materializedArtifactDir()
		if err != nil {
			t.Fatal(err)
		}
		identity, err := currentCompiledInterpreterIdentity()
		if err != nil {
			t.Fatal(err)
		}
		cacheRoot := os.Getenv(artifactCacheRootEnv)
		compiled, err := loadCompiledInterpreter(cacheRoot, identity, func() ([]byte, error) {
			file, err := os.OpenFile(filepath.Join(cacheRoot, "compile-count"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				return nil, err
			}
			_, writeErr := file.Write([]byte("x"))
			if err := errors.Join(writeErr, file.Close()); err != nil {
				return nil, err
			}
			return compileInterpreter(root)
		})
		if err != nil {
			t.Fatal(err)
		}
		engine := wasmtime.NewEngineWithConfig(newInterpreterConfig())
		module, err := wasmtime.NewModuleDeserialize(engine, compiled)
		if err != nil {
			engine.Close()
			t.Fatal(err)
		}
		module.Close()
		engine.Close()
		source := []byte("counter = 0\ndef handle(input):\n    global counter\n    counter += 1\n    return {\"counter\": counter}\n")
		request := Request{ModuleID: "isolated", RowID: "isolated", Source: source, Digest: digestSource(source), Entry: DefaultEntry, Input: []byte("{}"), Fuel: 2_000_000_000, MemoryPages: 2048, OutputBytes: 4096}
		for range 2 {
			result, err := Execute(context.Background(), request)
			if err != nil || string(result.Output) != `{"counter":1}` {
				t.Fatalf("fresh Store leaked model globals: %s %v", result.Output, err)
			}
		}
		fmt.Fprintln(os.Stdout, artifactCacheResultPrefix+identity.key())
		return
	}
	root := t.TempDir()
	for _, count := range []int{3, 2} {
		keys := runArtifactCacheHelpers(t, t.Name(), root, count)
		for _, key := range keys {
			if key != keys[0] {
				t.Fatal("same-engine publishers did not converge")
			}
		}
		marker, err := os.ReadFile(filepath.Join(root, "compile-count"))
		if err != nil || string(marker) != "x" {
			t.Fatalf("fresh workers recompiled instead of reusing code: %q %v", marker, err)
		}
	}
}

func TestCompiledInterpreterIdentityInventoryIsComplete(t *testing.T) {
	identity, err := currentCompiledInterpreterIdentity()
	if err != nil || identity.Executable == "" {
		t.Fatal(err)
	}
	value := reflect.ValueOf(&identity).Elem()
	keys := map[string]bool{identity.key(): true}
	for i := 0; i < value.NumField(); i++ {
		changed := identity
		field := reflect.ValueOf(&changed).Elem().Field(i)
		if field.Kind() == reflect.String {
			field.SetString(field.String() + "-different")
		} else {
			for j := 0; j < field.NumField(); j++ {
				policyChange := identity
				flag := reflect.ValueOf(&policyChange).Elem().Field(i).Field(j)
				flag.SetBool(!flag.Bool())
				if keys[policyChange.key()] {
					t.Fatalf("cache key omitted policy %s", field.Type().Field(j).Name)
				}
				keys[policyChange.key()] = true
			}
			continue
		}
		if keys[changed.key()] {
			t.Fatalf("cache key omitted %s", value.Type().Field(i).Name)
		}
		keys[changed.key()] = true
	}
}

func TestCompiledInterpreterRejectsCorruptForeignAndUnsafeEntries(t *testing.T) {
	identity, err := currentCompiledInterpreterIdentity()
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"truncated", "body", "identity", "size", "unknown", "trailing_header", "symlink", "shared_file", "shared_dir", "symlink_dir"} {
		t.Run(fault, func(t *testing.T) {
			root := t.TempDir()
			body := []byte("compiler-owned-test-bytes-never-deserialized")
			compileCount := 0
			compile := func() ([]byte, error) { compileCount++; return body, nil }
			if _, err := loadCompiledInterpreter(root, identity, compile); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(root, "compiled")
			name := filepath.Join(dir, identity.key()+".module")
			raw, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			line := bytes.IndexByte(raw, '\n')
			var header compiledInterpreterHeader
			if err := json.Unmarshal(raw[:line], &header); err != nil {
				t.Fatal(err)
			}
			unsafe := strings.Contains(fault, "symlink") || strings.HasPrefix(fault, "shared")
			switch fault {
			case "truncated":
				raw = raw[:line]
			case "body":
				raw[len(raw)-1] ^= 1
			case "identity":
				header.Identity.Engine += "-foreign"
			case "size":
				header.Size++
			case "unknown":
				raw = append([]byte(`{"unknown":1,`), raw[1:]...)
			case "trailing_header":
				raw = append(append(append([]byte{}, raw[:line]...), []byte(" {}\n")...), raw[line+1:]...)
			case "symlink":
				if err := os.Rename(name, name+".real"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(name+".real", name); err != nil {
					t.Fatal(err)
				}
			case "shared_file":
				if err := os.Chmod(name, 0o644); err != nil {
					t.Fatal(err)
				}
			case "shared_dir":
				if err := os.Chmod(dir, 0o755); err != nil {
					t.Fatal(err)
				}
			case "symlink_dir":
				if err := os.Rename(dir, dir+".real"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(dir+".real", dir); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "identity" || fault == "size" {
				encoded, err := json.Marshal(header)
				if err != nil {
					t.Fatal(err)
				}
				raw = append(append(encoded, '\n'), body...)
			}
			if !unsafe {
				if err := os.WriteFile(name, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := loadCompiledInterpreter(root, identity, compile)
			if unsafe {
				if err == nil || compileCount != 1 {
					t.Fatalf("unsafe cache admitted: %v count=%d", err, compileCount)
				}
			} else if err != nil || !bytes.Equal(got, body) || compileCount != 2 {
				t.Fatalf("corrupt/foreign code was not rejected and regenerated: %v count=%d", err, compileCount)
			}
		})
	}
}

func TestCompiledInterpreterFailedCompilationDoesNotPublish(t *testing.T) {
	identity, err := currentCompiledInterpreterIdentity()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	want := errors.New("compiler failed")
	if _, err := loadCompiledInterpreter(root, identity, func() ([]byte, error) { return nil, want }); !errors.Is(err, want) {
		t.Fatalf("compiler cause lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "compiled", identity.key()+".module")); !os.IsNotExist(err) {
		t.Fatalf("partial module published: %v", err)
	}
}

func TestCompiledInterpreterRefusesRelativeCacheAuthority(t *testing.T) {
	identity, err := currentCompiledInterpreterIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadCompiledInterpreter("relative-cache", identity, func() ([]byte, error) {
		t.Fatal("relative authority reached the compiler")
		return nil, nil
	}); err == nil {
		t.Fatal("relative cache root admitted")
	}
}

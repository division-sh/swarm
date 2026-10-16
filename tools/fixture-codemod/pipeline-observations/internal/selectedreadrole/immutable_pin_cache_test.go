package selectedreadrole

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestImmutableReadRolePinsReauthenticateCopiedBytesAndIsolateCallers(t *testing.T) {
	for _, tc := range []struct {
		path string
		read func(string) ([]EntityToolPin, error)
	}{
		{EntityToolPath, ReadEntityToolPins},
		{DeploymentResourcePath, ReadDeploymentResourcePins},
		{StandingIngressPath, ReadStandingIngressPins},
	} {
		t.Run(filepath.Base(tc.path), func(t *testing.T) {
			assertImmutableReadRolePins(t, tc.path, tc.read)
		})
	}
}

func assertImmutableReadRolePins(t *testing.T, relative string, read func(string) ([]EntityToolPin, error)) {
	t.Helper()
	root := t.TempDir()
	raw, err := os.ReadFile(filepath.Join("../../../../..", relative))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, relative)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	pins, err := read(root)
	if err != nil || len(pins) == 0 {
		t.Fatal("exact copied evidence refused:", err)
	}
	expected := append([]EntityToolPin(nil), pins...)
	pins[0].After = "foreign caller"
	if again, err := read(root); err != nil || !reflect.DeepEqual(again, expected) {
		t.Fatal("caller changed the immutable pin cache:", err)
	}
	if err := os.WriteFile(path, append(bytes.Clone(raw), ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := read(root); err == nil {
		t.Fatal("warm cache hid changed actual evidence")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := read(root); err == nil {
		t.Fatal("warm cache hid missing actual evidence")
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if again, err := read(root); err != nil || !reflect.DeepEqual(again, expected) {
		t.Fatal("hostile input poisoned authenticated evidence:", err)
	}
}

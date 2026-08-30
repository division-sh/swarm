package publicingress

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicIngressArchitectureRatchets(t *testing.T) {
	repo := filepath.Clean(filepath.Join("..", "..", ".."))
	registrationSource, err := os.ReadFile(filepath.Join(repo, "internal", "runtime", "publicingress", "registration.go"))
	if err != nil {
		t.Fatalf("read registration owner: %v", err)
	}
	if strings.Contains(strings.ToLower(string(registrationSource)), "telegram") {
		t.Fatal("provider registration owner contains a provider-specific branch")
	}
	for _, forbidden := range []string{
		"states        map[string]registrationState",
		"managedRoutes map[string]struct{}",
		"SetRegistration(",
		"ReplaceRegistrationKeys(",
	} {
		if strings.Contains(string(registrationSource), forbidden) {
			t.Fatalf("provider registration controller revived split snapshot owner %q", forbidden)
		}
	}
	for _, required := range []string{
		"registrationPhaseOutcomeUncertain",
		"terminalizePendingReadback",
	} {
		if !strings.Contains(string(registrationSource), required) {
			t.Fatalf("provider registration lifecycle owner is missing %q", required)
		}
	}
	readinessSource, err := os.ReadFile(filepath.Join(repo, "internal", "runtime", "publicingress", "readiness.go"))
	if err != nil {
		t.Fatalf("read registration snapshot owner: %v", err)
	}
	for _, required := range []string{"type RegistrationSnapshotOwner struct", "func (o *ReadinessOwner) CallbackCurrent", "currentRevision(process.revision)"} {
		if !strings.Contains(string(readinessSource), required) {
			t.Fatalf("registration snapshot owner is missing currentness ratchet %q", required)
		}
	}

	for _, root := range []string{
		filepath.Join(repo, "internal", "store", "migrations"),
		filepath.Join(repo, "internal", "store", "internal"),
	} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
				return nil
			}
			raw, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			lower := strings.ToLower(string(raw))
			for _, forbidden := range []string{
				"create table public_ingress",
				"create table provider_registration",
				"create table registration_intent",
				"create table registration_evidence",
			} {
				if strings.Contains(lower, forbidden) {
					t.Errorf("%s persists process-local observation via %q", path, forbidden)
				}
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("scan persistence tree %s: %v", root, err)
		}
	}

	spec, err := os.ReadFile(filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	for _, required := range []string{
		"provider_registration may allocate the next ordinal only after the preceding attempt settled terminal_failure with exact launch_rejected=true and zero provider dispatch",
		"Mismatch or unavailable readback terminalizes that same attempt as outcome_uncertain",
		"A live predecessor attempt or handle is never",
	} {
		if !strings.Contains(string(spec), required) {
			t.Fatalf("authoritative provider-registration contract is missing %q", required)
		}
	}
}

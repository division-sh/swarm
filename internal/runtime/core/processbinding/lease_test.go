package processbinding

import (
	"math"
	"testing"
)

func TestProcessLeaseStampValidation(t *testing.T) {
	valid := LeaseStamp{
		LeaseIdentity: LeaseIdentity{
			LeaseID: "6780a2b5-6864-47bc-b2bb-cadefcfdc871", OwnerID: "serve:holder",
			BootID: "6b89cf27-5260-4a49-a88e-c7ff4c5ac2c3", RuntimeInstanceID: "e67c38fa-7949-4770-8d90-063e4064c2d5",
			Backend: PostgresLeaseBackend,
		},
		Generation: 1,
	}
	for _, backend := range []string{PostgresLeaseBackend, SQLiteLeaseBackend} {
		candidate := valid
		candidate.Backend = backend
		if err := candidate.Validate(); err != nil {
			t.Fatalf("supported backend %s: %v", backend, err)
		}
	}
	for _, test := range []struct {
		name string
		edit func(*LeaseStamp)
	}{
		{"zero_generation", func(s *LeaseStamp) { s.Generation = 0 }},
		{"unpersistable_generation", func(s *LeaseStamp) { s.Generation = math.MaxInt64 + 1 }},
		{"missing_lease", func(s *LeaseStamp) { s.LeaseID = "" }},
		{"nil_lease", func(s *LeaseStamp) { s.LeaseID = "00000000-0000-0000-0000-000000000000" }},
		{"noncanonical_lease", func(s *LeaseStamp) { s.LeaseID = "6780A2B5-6864-47BC-B2BB-CADEFCFDC871" }},
		{"missing_boot", func(s *LeaseStamp) { s.BootID = "" }},
		{"missing_runtime", func(s *LeaseStamp) { s.RuntimeInstanceID = "" }},
		{"missing_owner", func(s *LeaseStamp) { s.OwnerID = "" }},
		{"padded_owner", func(s *LeaseStamp) { s.OwnerID = " serve:holder" }},
		{"unsupported_backend", func(s *LeaseStamp) { s.Backend = "recorded_owner" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.edit(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("invalid process lease admitted")
			}
		})
	}
}

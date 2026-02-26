package factory

import "testing"

func TestCandidateVerticalNames_CountAndFormat(t *testing.T) {
	out := candidateVerticalNames("NYC", 7)
	if len(out) != 7 {
		t.Fatalf("expected 7, got %d", len(out))
	}
	if out[0] == "" || out[0] == out[1] {
		t.Fatalf("unexpected output: %#v", out[:2])
	}
}

func TestDeriveVerticalNamesFromSignals_FallbackAndDedup(t *testing.T) {
	// No signals -> fallback.
	out := deriveVerticalNamesFromSignals(nil, "SF", 3)
	if len(out) != 3 {
		t.Fatalf("expected 3, got %d", len(out))
	}

	// With signals, dedup and classification.
	signals := []Signal{
		{Lead: "pet grooming in SF", Score: 90},
		{Lead: "PET services", Score: 80}, // duplicate class
		{Lead: "hvac repair", Score: 70},
	}
	out = deriveVerticalNamesFromSignals(signals, "SF", 3)
	if len(out) != 3 {
		t.Fatalf("expected 3, got %d out=%#v", len(out), out)
	}
	// Ensure pet and hvac are represented.
	foundPet, foundHVAC := false, false
	for _, n := range out {
		if n == "Pet Grooming Operations - SF" {
			foundPet = true
		}
		if n == "HVAC Service Workflow - SF" {
			foundHVAC = true
		}
	}
	if !foundPet || !foundHVAC {
		t.Fatalf("expected derived names to include pet+hvac, got %#v", out)
	}
}

func TestClassifyLeadAsVertical_Cases(t *testing.T) {
	cases := map[string]string{
		"pet grooming":     "Pet Grooming Operations",
		"DENTAL clinic":    "Dental Clinic Scheduling",
		"home CLEANing":    "Home Cleaning Dispatch",
		"HVAC tune up":     "HVAC Service Workflow",
		"auto detailing":   "Auto Detail Booking",
		"fitness coach":    "Fitness Studio Operations",
		"something else":   "Local Services Workflow",
		"":                 "Local Services Workflow",
		"random services":  "Local Services Workflow",
	}
	for lead, want := range cases {
		if got := classifyLeadAsVertical(lead); got != want {
			t.Fatalf("lead=%q got=%q want=%q", lead, got, want)
		}
	}
}


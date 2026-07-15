package managedexecution

import "testing"

func TestAdmissionSeparatesNormalAndSelectedExecutionAuthority(t *testing.T) {
	normal, err := New(KindNormalRuntime, "runtime-owner", 3, "", "actors", "bundle", nil)
	if err != nil {
		t.Fatalf("build normal admission: %v", err)
	}
	if !normal.AuthorizesNormal() || normal.AuthorizesSelected("runtime-owner", "", 3) {
		t.Fatalf("normal admission crossed execution classes: %#v", normal)
	}

	executionID := "00000000-0000-0000-0000-000000000201"
	runID := "00000000-0000-0000-0000-000000000202"
	selected, err := New(KindSelectedContractFork, executionID, 7, runID, "actors", "bundle", []string{
		"00000000-0000-0000-0000-000000000203",
		"00000000-0000-0000-0000-000000000203",
	})
	if err != nil {
		t.Fatalf("build selected admission: %v", err)
	}
	if !selected.AuthorizesSelected(executionID, runID, 7) {
		t.Fatalf("selected admission rejected exact authority: %#v", selected)
	}
	if selected.AuthorizesNormal() || selected.AuthorizesSelected(executionID, runID, 8) || selected.AuthorizesSelected(executionID, "00000000-0000-0000-0000-000000000204", 7) {
		t.Fatalf("selected admission accepted a mismatched authority: %#v", selected)
	}
	if len(selected.CapabilitySurfaceIDs) != 1 {
		t.Fatalf("normalized surface identities = %v, want one", selected.CapabilitySurfaceIDs)
	}
}

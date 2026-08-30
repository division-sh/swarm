package packs

import "testing"

func TestParseChannelRegistrationTargetPreservesCanonicalFlowIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      string
		wantFlow string
		wantErr  bool
	}{
		{name: "root", raw: "ingress:.:telegram", wantFlow: "."},
		{name: "nested", raw: "ingress:packages/channels:telegram", wantFlow: "packages/channels"},
		{name: "noncanonical nested", raw: "ingress:packages//channels:telegram", wantErr: true},
		{name: "escaping", raw: "ingress:../channels:telegram", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseChannelRegistrationTarget(test.raw)
			if test.wantErr {
				if err == nil {
					t.Fatalf("ParseChannelRegistrationTarget(%q) succeeded: %#v", test.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseChannelRegistrationTarget(%q): %v", test.raw, err)
			}
			if got.FlowPath != test.wantFlow || got.Provider != "telegram" {
				t.Fatalf("ParseChannelRegistrationTarget(%q) = %#v", test.raw, got)
			}
		})
	}
}

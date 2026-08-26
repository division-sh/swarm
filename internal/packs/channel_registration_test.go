package packs

import "testing"

func TestParseChannelRegistrationTargetPreservesCanonicalPackageIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		wantPkg string
		wantErr bool
	}{
		{name: "root", raw: "ingress:.:telegram-ingress:telegram", wantPkg: "."},
		{name: "nested", raw: "ingress:packages/channels:telegram-ingress:telegram", wantPkg: "packages/channels"},
		{name: "noncanonical nested", raw: "ingress:packages//channels:telegram-ingress:telegram", wantErr: true},
		{name: "escaping", raw: "ingress:../channels:telegram-ingress:telegram", wantErr: true},
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
			if got.PackageKey != test.wantPkg || got.FlowID != "telegram-ingress" || got.Provider != "telegram" {
				t.Fatalf("ParseChannelRegistrationTarget(%q) = %#v", test.raw, got)
			}
		})
	}
}

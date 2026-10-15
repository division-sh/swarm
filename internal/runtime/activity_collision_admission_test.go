package runtime

import (
	"maps"
	"reflect"
	"testing"
)

func TestActivityResultEventAdmissionPreservesExactCollisionAndRegistration(t *testing.T) {
	for _, tc := range []struct {
		name, event, wantError string
		authored, before, want map[string]string
	}{
		{name: "empty does not register", event: "  "},
		{
			name: "fresh normalized name registers exact owner", event: " send.succeeded ",
			before: map[string]string{}, want: map[string]string{"send.succeeded": "current"},
		},
		{
			name: "authored collision does not register", event: "send.succeeded",
			authored: map[string]string{"send.succeeded": "authored event"}, before: map[string]string{}, want: map[string]string{},
			wantError: `current: generated activity result event "send.succeeded" collides with authored event "send.succeeded"`,
		},
		{
			name: "generated collision preserves previous owner", event: "send.succeeded",
			before: map[string]string{"send.succeeded": "previous"}, want: map[string]string{"send.succeeded": "previous"},
			wantError: `current: generated activity result event "send.succeeded" collides with generated result event from previous`,
		},
		{
			name: "authored collision retains precedence", event: "send.succeeded",
			authored: map[string]string{"send.succeeded": "authored event"},
			before:   map[string]string{"send.succeeded": "previous"}, want: map[string]string{"send.succeeded": "previous"},
			wantError: `current: generated activity result event "send.succeeded" collides with authored event "send.succeeded"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authoredBefore := maps.Clone(tc.authored)
			generated := maps.Clone(tc.before)
			err := admitActivityResultEventName(tc.event, "current", tc.authored, generated)
			if (err == nil) != (tc.wantError == "") || err != nil && err.Error() != tc.wantError {
				t.Fatalf("admission error=%v, want %q", err, tc.wantError)
			}
			if !reflect.DeepEqual(generated, tc.want) || !reflect.DeepEqual(tc.authored, authoredBefore) {
				t.Fatalf("admission changed owner evidence: generated=%#v want=%#v authored=%#v", generated, tc.want, tc.authored)
			}
		})
	}
}

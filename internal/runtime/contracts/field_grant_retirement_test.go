package contracts

import (
	"fmt"
	"strings"
	"testing"
)

func TestFieldGrantAuthorityRetired(t *testing.T) {
	for direction, key := range map[string]string{"inputs": "reads", "outputs": "writes"} {
		for _, value := range []string{"null", "[]", "[entity.status]", "{status: text}"} {
			t.Run(direction+"/"+value, func(t *testing.T) {
				body := fmt.Sprintf("# fixture: grant-rejection\n%s:\n  %s: %s\n", direction, key, value)
				if _, err := admitSchemaFragment(body); err == nil || !strings.Contains(err.Error(), "schema.yaml:") {
					t.Fatalf("retired field grant admitted or source location lost: %v\n%s", err, body)
				}
				if bundle, err := loadSchemaFragment(t, body); err == nil || bundle != nil {
					t.Fatalf("retired field grant produced a bundle: %#v, error %v", bundle, err)
				}
			})
		}
	}
}

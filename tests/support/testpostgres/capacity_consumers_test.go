package testpostgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresCapacityRequirementConsumers(t *testing.T) {
	if RequiredMaxConnections != 300 {
		t.Fatalf("declared requirement changed: %d", RequiredMaxConnections)
	}
	for _, image := range []string{"image", "sha256:0123456789", "another-image"} {
		oldBytes := strings.Join([]string{image, "postgres:16", "tmpfs-pgdata", "random-loopback-port", "max_connections=300", "fsync=off", "synchronous_commit=off", "full_page_writes=off"}, "\x00")
		hash := sha256.Sum256([]byte(oldBytes))
		if actual, want := serviceSpecHash(image), hex.EncodeToString(hash[:]); actual != want {
			t.Fatalf("Docker service identity changed: %s, want %s", actual, want)
		}
	}
	for _, test := range []struct {
		name    string
		setting string
		value   string
	}{
		{name: "exact_owned_settings"},
		{name: "below_capacity", setting: "max_connections", value: "299"},
		{name: "above_capacity", setting: "max_connections", value: "301"},
		{name: "fsync", setting: "fsync", value: "on"},
		{name: "synchronous_commit", setting: "synchronous_commit", value: "on"},
		{name: "full_page_writes", setting: "full_page_writes", value: "on"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.MatchExpectationsInOrder(false)
			for setting, value := range map[string]string{"max_connections": "300", "fsync": "off", "synchronous_commit": "off", "full_page_writes": "off"} {
				if setting == test.setting {
					value = test.value
				}
				mock.ExpectQuery(`^SELECT current_setting\(\$1\)$`).WithArgs(setting).WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(value))
			}
			err = verifyOwnedPostgresSettings(context.Background(), db)
			if (err != nil) != (test.setting != "") {
				t.Fatalf("owned settings admission: %v", err)
			}
			if test.setting == "" {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			} else if !strings.Contains(err.Error(), test.setting) {
				t.Fatalf("wrong settings refusal: %v", err)
			}
		})
	}
}

func capacityOwnerConsumers(t *testing.T, source string) map[string]int {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "service_registry.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if identifier, ok := node.(*ast.Ident); ok && identifier.Name == "RequiredMaxConnections" {
				counts[function.Name.Name]++
			}
			return true
		})
	}
	return counts
}

func TestPostgresCapacityConsumerGuard(t *testing.T) {
	source, err := os.ReadFile("service_registry.go")
	if err != nil {
		t.Fatal(err)
	}
	counts := capacityOwnerConsumers(t, string(source))
	for _, consumer := range []string{"RunCreator", "serviceSpecHash", "verifyOwnedPostgresSettings"} {
		if counts[consumer] != 1 {
			t.Fatalf("%s bypasses the shared capacity owner: %d reads", consumer, counts[consumer])
		}
	}
	// A literal restored to any consumer must be caught, even while its current
	// numeric value happens to agree with the owner.
	for index, consumer := range []string{"RunCreator", "serviceSpecHash", "verifyOwnedPostgresSettings"} {
		parts := strings.Split(string(source), "RequiredMaxConnections")
		if len(parts) != 4 {
			t.Fatal("capacity consumer inventory changed")
		}
		parts[index] += "300" + parts[index+1]
		parts = append(parts[:index+1], parts[index+2:]...)
		mutated := strings.Join(parts, "RequiredMaxConnections")
		if capacityOwnerConsumers(t, mutated)[consumer] != 0 {
			t.Fatalf("guard failed to detect %s literal regression", consumer)
		}
	}
}

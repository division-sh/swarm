package mutationprotocol

import (
	"strings"
	"testing"

	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

func runIdentityAliases(runID string) []string {
	hex := strings.ReplaceAll(runID, "-", "")
	return []string{
		strings.ToUpper(runID), runID, hex, "{" + runID + "}",
		strings.Join([]string{hex[:4], hex[4:8], hex[8:12], hex[12:16], hex[16:20], hex[20:24], hex[24:28], hex[28:]}, "-"),
		"{" + hex + "}",
	}
}

func TestPhysicalRunKeyPostgresUUIDForms(t *testing.T) {
	const runID = "aabbccdd-eeff-0011-2233-445566778899"
	hex := strings.ReplaceAll(runID, "-", "")
	// Every subset of PG's seven optional hyphens, with and without braces.
	for mask := 0; mask < 128; mask++ {
		var body strings.Builder
		for group := 0; group < 8; group++ {
			if group > 0 && mask&(1<<(group-1)) != 0 {
				body.WriteByte('-')
			}
			body.WriteString(hex[4*group : 4*(group+1)])
		}
		for _, input := range []string{body.String(), strings.ToUpper(body.String()), "{" + body.String() + "}", "{" + strings.ToUpper(body.String()) + "}"} {
			key, valid := physicalRunKey(privateactivity.DialectPostgres, " \t"+input+"\n")
			if !valid || key != runID {
				t.Fatalf("PG input %q: key=%q valid=%v", input, key, valid)
			}
			key, valid = physicalRunKey(privateactivity.DialectSQLite, " \t"+input+"\n")
			if !valid || key != input {
				t.Fatalf("SQLite input %q: key=%q valid=%v", input, key, valid)
			}
		}
	}
}

func TestPhysicalRunKeyInvalidUUIDAndExactText(t *testing.T) {
	const runID = "aabbccdd-eeff-0011-2233-445566778899"
	for _, input := range []string{
		"", " \t", "run", "urn:uuid:" + runID, "[" + runID + "]", "x" + runID + "x",
		"{" + runID, runID + "}", "{{" + runID + "}}", "{}",
		"-" + runID, runID + "-", strings.Replace(runID, "-", "--", 1),
		"aa-bbccddeeff00112233445566778899", "aabbccdd eeff00112233445566778899",
		"gabbccdd-eeff-0011-2233-445566778899", runID[:len(runID)-1], runID + "0",
	} {
		key, valid := physicalRunKey(privateactivity.DialectPostgres, input)
		if valid || key != strings.TrimSpace(input) {
			t.Fatalf("invalid PG input %q: key=%q valid=%v", input, key, valid)
		}
		key, valid = physicalRunKey(privateactivity.DialectSQLite, input)
		if key != strings.TrimSpace(input) || valid != (key != "") {
			t.Fatalf("SQLite TEXT input %q: key=%q valid=%v", input, key, valid)
		}
	}
}

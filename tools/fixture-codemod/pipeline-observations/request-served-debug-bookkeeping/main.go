package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

type site struct {
	Kind, File, Declaration, Operation, Resolved, Family, Owner string
	Multiplicity                                                int
}
type pair struct {
	File, Enclosing, Operation, Kind, Family, Owner, Before, After string
	Multiplicity                                                   int
}

func main() {
	model := len(os.Args) == 3 && os.Args[1] == "-go-model"
	if len(os.Args) != 2 && !model {
		panic("usage: request-served-debug-bookkeeping [-go-model] <exact-candidate-census.tsv>")
	}
	predecessor, err := exec.Command("git", "show", "bfdcdc112f7053494ea597c053aaa49678804bbc:internal/store/testdata/persistence_authority_debt_baseline.tsv").Output()
	must(err)
	if fmt.Sprintf("%x", sha256.Sum256(predecessor)) != "fb6db8b87f441912b61afded6ea18eec5238520d672a5cb46a63c87da2f31bd5" {
		panic("approved complete predecessor baseline changed")
	}
	old := parse(predecessor)
	current := read(os.Args[len(os.Args)-1])
	byLocation := map[string][]site{}
	for _, s := range old {
		byLocation[location(s)] = append(byLocation[location(s)], s)
	}
	var pairs []pair
	total := 0
	for key, after := range current {
		if before, ok := old[key]; ok && before == after {
			continue
		}
		matches := byLocation[location(after)]
		if len(matches) != 1 {
			panic("missing/ambiguous exact donor: " + location(after))
		}
		before := matches[0]
		validate(before, after)
		if _, survives := current[identity(before)]; survives {
			panic("old and new identities coexist")
		}
		pairs = append(pairs, pair{after.File, after.Declaration, after.Operation, after.Kind, after.Family, after.Owner, before.Resolved, after.Resolved, after.Multiplicity})
		total += after.Multiplicity
	}
	if len(pairs) != 49 || total != 71 {
		panic(fmt.Sprintf("unexpected exact held transfers: %d identities/%d occurrences", len(pairs), total))
	}
	sort.Slice(pairs, func(i, j int) bool {
		a, b := pairs[i], pairs[j]
		return a.File+"\t"+a.Enclosing+"\t"+a.Operation < b.File+"\t"+b.Enclosing+"\t"+b.Operation
	})
	if model {
		writeModel(pairs)
		return
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	must(encoder.Encode(pairs))
}

func writeModel(pairs []pair) {
	fmt.Println("// Ruling6138883675: exactly49 pairs retain71 raw occurrences, with zero credit.")
	fmt.Println("func debtServedDebugSignatureTransitions() [49][2]authorityDebtSite {")
	fmt.Println("return [49][2]authorityDebtSite{")
	for _, p := range pairs {
		fmt.Println("{")
		for _, resolved := range []string{p.Before, p.After} {
			fmt.Printf("{Kind:%q, File:%q, Declaration:%q, Operation:%q, Resolved:%q, Family:%q, Replacement:%q, Multiplicity:%d},\n", p.Kind, p.File, p.Enclosing, p.Operation, resolved, p.Family, p.Owner, p.Multiplicity)
		}
		fmt.Println("},")
	}
	fmt.Println("}\n}")
}

func validate(before, after site) {
	if after.Kind != "raw-operation" || !strings.HasPrefix(after.File, "internal/serveapp/") || before.Multiplicity != after.Multiplicity || before.Family != after.Family || before.Owner != after.Owner {
		panic("changed debt classification/owner/multiplicity")
	}
	const role = "github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader"
	if !strings.Contains(before.Resolved, "*database/sql.DB") || strings.Contains(before.Resolved, role) || strings.Count(after.Resolved, role) != 2 {
		panic("not the exact added native reader parameter/argument")
	}
	inverse := strings.Replace(after.Resolved, ", reader "+role, "", 1)
	inverse = strings.Replace(inverse, "|arg="+role, "", 1)
	if inverse != before.Resolved {
		panic("non-reader signature/receiver/argument/result changed")
	}
}

func read(path string) map[string]site {
	raw, err := os.ReadFile(path)
	must(err)
	return parse(raw)
}

func parse(raw []byte) map[string]site {
	out := map[string]site{}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 8 {
			panic("incomplete candidate census row")
		}
		n, err := strconv.Atoi(fields[7])
		must(err)
		if n <= 0 {
			panic("invalid multiplicity")
		}
		s := site{fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], n}
		key := identity(s)
		if _, duplicate := out[key]; duplicate {
			panic("duplicate candidate occurrence")
		}
		out[key] = s
	}
	return out
}
func identity(s site) string {
	return strings.Join([]string{s.Kind, s.File, s.Declaration, s.Operation, s.Resolved, s.Family, s.Owner}, "\t")
}
func location(s site) string {
	return strings.Join([]string{s.Kind, s.File, s.Declaration, s.Operation}, "\t")
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}

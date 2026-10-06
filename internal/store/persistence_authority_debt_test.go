package store_test

import (
	"archive/tar"
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

const debtBaselinePath = "internal/store/testdata/persistence_authority_debt_baseline.tsv"
const debtRefreshEnv = "SWARM_REFRESH_PERSISTENCE_AUTHORITY_DEBT"

// The reviewed, unlanded bootstrap cannot move when its guard PR is rebased.
const debtBootstrapSource = "52b954ec26c85a47605cefb8d7030f2a842f8c24"

type authorityDebtSite struct {
	Kind, File, Declaration, Operation, Resolved, Family, Replacement string
	Multiplicity                                                      int
}

func (site authorityDebtSite) identity() string {
	return strings.Join([]string{site.Kind, site.File, site.Declaration, site.Operation, site.Resolved, site.Family, site.Replacement}, "\t")
}

type authorityDebtBaseline struct {
	BootstrapSource, Collector string
	Sites                      map[string]authorityDebtSite
}

func authorityDebtOperation(member string) string {
	if cut := strings.LastIndex(member, "#"); cut >= 0 {
		if _, err := strconv.Atoi(member[cut+1:]); err == nil {
			return member[:cut]
		}
	}
	return member
}

func authorityDebtRoute(finding authorityFinding) (string, string) {
	path, operation := finding.File, finding.Member
	if finding.Kind == "unresolved-excluded-source" {
		return "excluded-source-uncertainty", "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase"
	}
	if finding.Kind == "selected-boundary" {
		return "selected-construction", "selected construction/work-lifetime owner; do not recover a concrete store"
	}
	if finding.Kind == "forbidden-test-consumption" || finding.Kind == "inactive-test-consumption" {
		return "fixture-confinement", "named private evidence/fault operation; no generic fixture import"
	}
	if strings.Contains(operation, "Database") || strings.HasPrefix(finding.Kind, "context-") || strings.Contains(finding.Kind, "construction") || strings.Contains(finding.Kind, "export") {
		return "authority-carrier", "original selected construction/work-lifetime owner; no getter or transaction-context recovery"
	}
	if strings.Contains(path, "pipeline") {
		return "workflow", "selected workflow/construction owner; domain observation port for readback"
	}
	if strings.Contains(path, "runfork") || strings.Contains(path, "run_fork") {
		return "fork", "selected fork/source/event/lifecycle operation and bounded fork observation"
	}
	if strings.Contains(path, "mailbox") {
		return "mailbox", "selected mailbox/completion owner and named mailbox fault/readback"
	}
	if strings.Contains(path, "event") || strings.Contains(operation, "Event") {
		return "event", "selected canonical event operation and domain event observation port"
	}
	if strings.Contains(path, "run") || strings.Contains(operation, "Run") {
		return "lifecycle", "selected run lifecycle/candidate/source owner and bounded lifecycle observation"
	}
	if strings.Contains(path, "channel") {
		return "channel", "selected channel/delivery owner and named channel fault/readback"
	}
	if strings.Contains(operation, "Query") || strings.Contains(operation, "Scan") || strings.Contains(operation, "Next") {
		return "observation", "domain observation port on the exact selected owner; missing exact port: escalate #2542"
	}
	return "unowned-authority", "exact domain owner not identified here; escalate #2542, never invent a SQL escape"
}

func authorityDebtSites(findings []authorityFinding) map[string]authorityDebtSite {
	sites := map[string]authorityDebtSite{}
	for _, finding := range findings {
		if finding.Kind != "unresolved-excluded-source" && finding.Kind != "selected-boundary" && finding.Kind != "selected-store-construction" && finding.Kind != "forbidden-test-consumption" && finding.Kind != "inactive-test-consumption" && (!finding.RawSQL || debtRawSQLDispositionAllowed(finding, "fixture-2151")) {
			continue
		}
		family, replacement := authorityDebtRoute(finding)
		site := authorityDebtSite{Kind: finding.Kind, File: finding.File, Declaration: finding.Enclosing, Operation: authorityDebtOperation(finding.Member), Resolved: finding.Resolved, Family: family, Replacement: replacement, Multiplicity: 1}
		key := site.identity()
		if prior, ok := sites[key]; ok {
			site.Multiplicity = prior.Multiplicity + 1
		}
		sites[key] = site
	}
	return sites
}

func authorityDebtCount(sites map[string]authorityDebtSite) int {
	total := 0
	for _, site := range sites {
		total += site.Multiplicity
	}
	return total
}

func authorityDebtSubset(actual, allowed map[string]authorityDebtSite, label string) []string {
	var failures []string
	for key, site := range actual {
		limit := allowed[key].Multiplicity
		if site.Multiplicity > limit {
			failures = append(failures, fmt.Sprintf("%s new/increased site: %s declaration=%s operation=%s kind=%s family=%s old=%d new=%d owner=%s", label, site.File, site.Declaration, site.Operation, site.Kind, site.Family, limit, site.Multiplicity, site.Replacement))
		}
	}
	sort.Strings(failures)
	return failures
}

func authorityDebtRatchet(actual, head, base, baseActual map[string]authorityDebtSite) []string {
	failures := authorityDebtSubset(actual, head, "actual -> head baseline")
	failures = append(failures, authorityDebtSubset(head, base, "head -> trusted baseline")...)
	failures = append(failures, authorityDebtSubset(actual, baseActual, "actual -> trusted source (resurrection)")...)
	return failures
}

func parseAuthorityDebtBaseline(data []byte) (authorityDebtBaseline, error) {
	baseline := authorityDebtBaseline{Sites: map[string]authorityDebtSite{}}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	var prior string
	schema, header := false, false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "# persistence-authority-debt-v1":
			if schema {
				return baseline, fmt.Errorf("duplicate debt schema")
			}
			schema = true
		case strings.HasPrefix(line, "# bootstrap-source="):
			if baseline.BootstrapSource != "" {
				return baseline, fmt.Errorf("duplicate bootstrap source")
			}
			baseline.BootstrapSource = strings.TrimPrefix(line, "# bootstrap-source=")
		case strings.HasPrefix(line, "# collector="):
			if baseline.Collector != "" {
				return baseline, fmt.Errorf("duplicate collector")
			}
			baseline.Collector = strings.TrimPrefix(line, "# collector=")
		case line == "# kind\tfile\tdeclaration\toperation\tresolved\tfamily\treplacement-owner\tmultiplicity":
			if header {
				return baseline, fmt.Errorf("duplicate debt header")
			}
			header = true
		case line == "":
			return baseline, fmt.Errorf("blank debt baseline row")
		case strings.HasPrefix(line, "#"):
			return baseline, fmt.Errorf("unknown debt baseline metadata %q", line)
		default:
			parts := strings.Split(line, "\t")
			if len(parts) != 8 {
				return baseline, fmt.Errorf("corrupt debt baseline row: %q", line)
			}
			count, err := strconv.Atoi(parts[7])
			if err != nil || count <= 0 {
				return baseline, fmt.Errorf("invalid debt multiplicity %q", parts[7])
			}
			for _, part := range parts[:7] {
				if part == "" || strings.ContainsAny(part, "\r\n") {
					return baseline, fmt.Errorf("missing debt identity field")
				}
			}
			if !filepath.IsLocal(parts[1]) || filepath.ToSlash(parts[1]) != parts[1] {
				return baseline, fmt.Errorf("invalid debt consumer path")
			}
			site := authorityDebtSite{parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], count}
			key := site.identity()
			if prior != "" && key <= prior {
				return baseline, fmt.Errorf("duplicate or unsorted debt site %q", key)
			}
			prior = key
			baseline.Sites[key] = site
		}
	}
	if err := scanner.Err(); err != nil {
		return baseline, err
	}
	if !schema || !header || !authorityDebtHex(baseline.BootstrapSource, 40) || !authorityDebtHex(baseline.Collector, 64) {
		return baseline, fmt.Errorf("missing/corrupt debt schema, source or collector identity")
	}
	return baseline, nil
}

func authorityDebtHex(value string, size int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(value) == size && len(decoded) == size/2 && strings.ToLower(value) == value
}

func marshalAuthorityDebtBaseline(baseline authorityDebtBaseline) []byte {
	var out strings.Builder
	fmt.Fprintf(&out, "# persistence-authority-debt-v1\n# bootstrap-source=%s\n# collector=%s\n# kind\tfile\tdeclaration\toperation\tresolved\tfamily\treplacement-owner\tmultiplicity\n", baseline.BootstrapSource, baseline.Collector)
	keys := make([]string, 0, len(baseline.Sites))
	for key := range baseline.Sites {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&out, "%s\t%d\n", key, baseline.Sites[key].Multiplicity)
	}
	return []byte(out.String())
}

func debtCollectorDigest(root string) (string, error) {
	// Include the whole census implementation and the ratchet/scope policy.
	// Hostile tests and generated debt rows do not define the permission model.
	hash := sha256.New()
	for _, path := range []string{"internal/store/persistence_authority_debt_census_test.go", "internal/store/persistence_authority_debt_test.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
		if err != nil {
			return "", err
		}
		var canonical bytes.Buffer
		if err := format.Node(&canonical, token.NewFileSet(), file); err != nil {
			return "", err
		}
		fmt.Fprintf(hash, "%s\x00%s\x00", path, canonical.Bytes())
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func debtGit(root string, args ...string) ([]byte, error) {
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %v: %w: %s", args, err, output)
	}
	return output, nil
}

func debtTrustedBase(root string) (string, error) {
	target := "origin/master"
	candidate := ""
	if eventPath := os.Getenv("GITHUB_EVENT_PATH"); eventPath != "" {
		data, err := os.ReadFile(eventPath)
		if err != nil {
			return "", err
		}
		var event struct {
			Before      string `json:"before"`
			After       string `json:"after"`
			PullRequest *struct {
				Base struct {
					SHA string `json:"sha"`
				} `json:"base"`
				Head struct {
					SHA string `json:"sha"`
				} `json:"head"`
			} `json:"pull_request"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			return "", err
		}
		if event.PullRequest != nil {
			target = event.PullRequest.Base.SHA
			candidate = event.PullRequest.Head.SHA
			if !authorityDebtHex(target, 40) || !authorityDebtHex(candidate, 40) {
				return "", fmt.Errorf("missing authenticated PR base/head identity")
			}
		} else if event.Before != "" || event.After != "" {
			if !authorityDebtHex(event.Before, 40) || !authorityDebtHex(event.After, 40) {
				return "", fmt.Errorf("missing authenticated push predecessor/head identity")
			}
			head, err := debtGit(root, "rev-parse", "HEAD")
			if err != nil {
				return "", err
			}
			if strings.TrimSpace(string(head)) != event.After {
				return "", fmt.Errorf("push event does not name the tested head")
			}
			target = event.Before
		}
	}
	// Hosted proof checkouts are shallow. Retrieve history, not a caller-chosen
	// baseline file; qualification remains bound to the actual merge base.
	shallow, err := debtGit(root, "rev-parse", "--is-shallow-repository")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(shallow)) == "true" {
		if _, err := debtGit(root, "fetch", "--no-tags", "--unshallow", "origin"); err != nil {
			return "", err
		}
	}
	if _, err := debtGit(root, "cat-file", "-e", target+"^{commit}"); err != nil {
		if !authorityDebtHex(target, 40) {
			return "", err
		}
		if _, err := debtGit(root, "fetch", "--no-tags", "origin", target); err != nil {
			return "", err
		}
	}
	comparison := "HEAD"
	if candidate != "" {
		entries, err := debtGit(root, "ls-tree", target, "--", debtBaselinePath)
		if err != nil {
			return "", err
		}
		if len(bytes.TrimSpace(entries)) == 0 {
			history, err := debtGit(root, "log", "-1", "--format=%H", target, "--", debtBaselinePath)
			if err != nil {
				return "", err
			}
			if len(bytes.TrimSpace(history)) != 0 {
				return "", fmt.Errorf("landed debt baseline disappeared from authenticated lineage; rebootstrap refused")
			}
			// Initial bootstrap is tied to the candidate's extraction base, not
			// GitHub's synthetic merge. Once landed, the tested integration tree
			// consumes the already-landed baseline without a rebootstrap.
			if _, err := debtGit(root, "cat-file", "-e", candidate+"^{commit}"); err != nil {
				if _, err := debtGit(root, "fetch", "--no-tags", "origin", candidate); err != nil {
					return "", err
				}
			}
			comparison = candidate
		}
	}
	base, err := debtGit(root, "merge-base", comparison, target)
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(string(base))
	if head, err := debtGit(root, "rev-parse", "HEAD"); err != nil {
		return "", err
	} else if sha == strings.TrimSpace(string(head)) {
		// A local checkout at master still needs an independent predecessor.
		parent, err := debtGit(root, "rev-parse", "HEAD^")
		if err != nil {
			return "", err
		}
		sha = strings.TrimSpace(string(parent))
	}
	if !authorityDebtHex(sha, 40) {
		return "", fmt.Errorf("invalid trusted integration base")
	}
	return sha, nil
}

func materializeDebtBase(t *testing.T, root, sha string) string {
	t.Helper()
	directory := t.TempDir()
	command := exec.Command("git", "-C", root, "archive", "--format=tar", sha)
	pipe, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(pipe)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			command.Process.Kill()
			command.Wait()
			t.Fatal(err)
		}
		if !filepath.IsLocal(header.Name) {
			command.Process.Kill()
			command.Wait()
			t.Fatalf("nonlocal trusted archive member %s", header.Name)
		}
		path := filepath.Join(directory, header.Name)
		switch header.Typeflag {
		case tar.TypeXGlobalHeader, tar.TypeXHeader:
			// git archive records its source commit in a PAX metadata header.
			continue
		case tar.TypeDir:
			err = os.MkdirAll(path, 0755)
		case tar.TypeReg:
			err = os.MkdirAll(filepath.Dir(path), 0755)
			if err != nil {
				break
			}
			var file *os.File
			file, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(header.Mode)&0777)
			if err != nil {
				break
			}
			_, err = io.Copy(file, reader)
			err = errors.Join(err, file.Close())
		case tar.TypeSymlink:
			err = os.MkdirAll(filepath.Dir(path), 0755)
			if err == nil {
				err = os.Symlink(header.Linkname, path)
			}
		default:
			err = fmt.Errorf("unsupported trusted archive entry %s", header.Name)
		}
		if err != nil {
			command.Process.Kill()
			command.Wait()
			t.Fatal(err)
		}
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("archive trusted base: %v: %s", err, stderr.String())
	}
	return directory
}

func debtCheckBootstrapAncestry(root, origin, integrationBase string) error {
	if !authorityDebtHex(origin, 40) {
		return fmt.Errorf("invalid bootstrap origin")
	}
	base, err := debtGit(root, "merge-base", origin, integrationBase)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(base)) != origin {
		return fmt.Errorf("bootstrap origin is not an ancestor of the trusted integration base")
	}
	return nil
}

func TestPersistenceAuthorityDebtRatchet(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	base, err := debtTrustedBase(root)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := debtCollectorDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	baseRoot := materializeDebtBase(t, root, base)
	baseFindings := debtLoadPersistenceAuthorityFindings(t, baseRoot)
	baseFindings = append(baseFindings, debtSelectedBoundaryFindings(t, baseRoot)...)
	baseActual := authorityDebtSites(baseFindings)
	headFindings := debtLoadPersistenceAuthorityFindings(t, root)
	headFindings = append(headFindings, debtSelectedBoundaryFindings(t, root)...)
	actual := authorityDebtSites(headFindings)
	baseBytes, baseErr := os.ReadFile(filepath.Join(baseRoot, debtBaselinePath))
	headBytes, headErr := os.ReadFile(filepath.Join(root, debtBaselinePath))
	refresh := os.Getenv(debtRefreshEnv)
	if refresh != "" && refresh != "downward" {
		t.Fatalf("%s supports only downward", debtRefreshEnv)
	}
	if refresh != "" && (os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "") {
		t.Fatal("normal CI is read-only; debt refresh refused")
	}
	bootstrap := os.IsNotExist(baseErr)
	if bootstrap {
		history, err := debtGit(root, "log", "-1", "--format=%H", base, "--", debtBaselinePath)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(history)) != "" {
			t.Fatal("landed debt baseline disappeared from trusted lineage; rebootstrap refused")
		}
	}
	var trusted, head authorityDebtBaseline
	if bootstrap {
		if err := debtCheckBootstrapAncestry(root, debtBootstrapSource, base); err != nil {
			t.Fatal(err)
		}
		originActual := baseActual
		if base != debtBootstrapSource {
			originRoot := materializeDebtBase(t, root, debtBootstrapSource)
			originFindings := debtLoadPersistenceAuthorityFindings(t, originRoot)
			originFindings = append(originFindings, debtSelectedBoundaryFindings(t, originRoot)...)
			originActual = authorityDebtSites(originFindings)
		}
		trusted = authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: collector, Sites: originActual}
		if headErr != nil {
			if !os.IsNotExist(headErr) || refresh != "downward" {
				t.Fatalf("initial debt baseline missing: %v; explicit downward bootstrap required", headErr)
			}
			head = trusted
		} else {
			head, err = parseAuthorityDebtBaseline(headBytes)
			if err != nil {
				t.Fatal(err)
			}
		}
	} else {
		if baseErr != nil {
			t.Fatal(baseErr)
		}
		trusted, err = parseAuthorityDebtBaseline(baseBytes)
		if err != nil {
			t.Fatal(err)
		}
		baseCollector, err := debtCollectorDigest(baseRoot)
		if err != nil {
			t.Fatal(err)
		}
		if headErr != nil {
			t.Fatalf("landed debt baseline cannot be removed or bootstrapped again: %v", headErr)
		}
		head, err = parseAuthorityDebtBaseline(headBytes)
		if err != nil {
			t.Fatal(err)
		}
		if err := debtValidateCollectorIdentity(baseCollector, collector, trusted, head); err != nil {
			t.Fatal(err)
		}
	}
	if head.BootstrapSource != trusted.BootstrapSource || head.Collector != collector {
		t.Fatal("debt baseline was reseeded or changed collector identity")
	}
	if bootstrap && !debtSitesEqual(head.Sites, trusted.Sites) {
		t.Fatal("bootstrap baseline must equal actual extraction-base debt exactly")
	}
	failures := authorityDebtRatchet(actual, head.Sites, trusted.Sites, baseActual)
	if len(failures) > 0 {
		t.Fatalf("authority debt ratchet failed: base=%s old=%d head=%d actual=%d\n%s", base, authorityDebtCount(trusted.Sites), authorityDebtCount(head.Sites), authorityDebtCount(actual), strings.Join(failures, "\n"))
	}
	if refresh == "downward" {
		removed := authorityDebtCount(head.Sites) - authorityDebtCount(actual)
		for _, key := range debtSortedKeys(head.Sites) {
			before := head.Sites[key]
			after := actual[key].Multiplicity
			if after < before.Multiplicity {
				t.Logf("debt removed=%d %s", before.Multiplicity-after, key)
			}
		}
		head.Sites = actual
		if err := os.WriteFile(filepath.Join(root, debtBaselinePath), marshalAuthorityDebtBaseline(head), 0644); err != nil {
			t.Fatal(err)
		}
		t.Logf("downward-only refresh removed=%d added=0", removed)
	}
	rawSites := 0
	for _, finding := range headFindings {
		if finding.Kind == "raw-operation" {
			rawSites++
		}
	}
	unresolved := 0
	confirmedRaw := 0
	for _, site := range actual {
		if site.Kind == "unresolved-excluded-source" {
			unresolved += site.Multiplicity
		} else if site.Kind == "raw-operation" {
			confirmedRaw += site.Multiplicity
		}
	}
	t.Logf("debt source=%s collector=%s total-findings=%d raw-operation-sites=%d debt=%d inherited-baseline=%d confirmed-raw-operation-debt=%d unresolved-excluded-occurrences=%d", base, collector, len(headFindings), rawSites, authorityDebtCount(actual), authorityDebtCount(head.Sites), confirmedRaw, unresolved)
}

func debtSortedKeys(sites map[string]authorityDebtSite) []string {
	keys := make([]string, 0, len(sites))
	for key := range sites {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
func debtValidateCollectorIdentity(base, current string, trusted, head authorityDebtBaseline) error {
	if head.BootstrapSource != trusted.BootstrapSource {
		return fmt.Errorf("debt baseline bootstrap source changed")
	}
	if base == current && trusted.Collector == current && head.Collector == current {
		return nil
	}
	if base == debtG01CollectorFrom && trusted.Collector == debtG01CollectorFrom &&
		current == debtG01CollectorTo && head.Collector == debtG01CollectorTo &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil
	}
	return fmt.Errorf("authority census/role policy changed outside the exact reviewed G01 transition; silent scan narrowing is forbidden")
}

func debtSitesEqual(left, right map[string]authorityDebtSite) bool {
	if len(left) != len(right) {
		return false
	}
	for key, site := range left {
		if right[key] != site {
			return false
		}
	}
	return true
}

func debtSelectedBoundaryFindings(t *testing.T, root string) []authorityFinding {
	t.Helper()
	var findings []authorityFinding
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "testdata", "vendor", "node_modules", ".swarm":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		findings = append(findings, debtSelectedBoundarySource(relative, file)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func debtSelectedBoundarySource(path string, file *ast.File) []authorityFinding {
	insideStore := strings.HasPrefix(path, "internal/store/")
	insideServe := strings.HasPrefix(path, "internal/serveapp/")
	insideSelected := strings.HasPrefix(path, "internal/store/selected/")
	insideInfrastructure := strings.HasPrefix(path, "internal/testpostgres/") || filepath.ToSlash(filepath.Dir(path)) == "internal/testutil"
	selectedConsumer := insideSelected || insideServe || strings.HasPrefix(path, "internal/cliapp/")
	constructionConsumer := insideSelected || filepath.ToSlash(filepath.Dir(path)) == "internal/store" || strings.HasPrefix(path, "internal/store/construction/") || strings.HasPrefix(path, "internal/store/internal/")
	imports := map[string]string{}
	var findings []authorityFinding
	add := func(declaration, operation, reason string) {
		findings = append(findings, authorityFinding{Kind: "selected-boundary", File: path, Enclosing: declaration, Member: operation, Resolved: reason, RawSQL: true})
	}
	for _, imp := range file.Imports {
		name, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			panic(err)
		}
		alias := filepath.Base(name)
		if imp.Name != nil {
			alias = imp.Name.Name
		}
		imports[alias] = name
		switch name {
		case "github.com/division-sh/swarm/internal/store/selected":
			if !selectedConsumer {
				add(file.Name.Name, "import:"+name, "selected owner outside process construction/inspection roles")
			}
		case "github.com/division-sh/swarm/internal/store/construction":
			if !constructionConsumer {
				add(file.Name.Name, "import:"+name, "direct construction outside selected construction roles")
			}
		case "reflect":
			if !strings.HasSuffix(path, "_test.go") && (insideServe || insideSelected) {
				add(file.Name.Name, "import:reflect", "reflection in selected-store production projection")
			}
		}
	}
	retired := map[string]bool{"storeBundle": true, "selectedConcreteRuntimeStore": true, "selectedRuntimeStoreFacade": true, "selectedStoreBundleRoleLedger": true, "validateSelectedStoreBundleRoles": true, "APIOptionalCapabilityBuilder": true, "configuredRunFork": true, "BundleWriter": true}
	for _, decl := range file.Decls {
		name := file.Name.Name
		if fn, ok := decl.(*ast.FuncDecl); ok {
			name = fn.Name.Name
		}
		ast.Inspect(decl, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && (insideServe || insideSelected) && retired[id.Name] {
				add(name, "retired:"+id.Name, "retired selected-store interpreter")
			}
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			qualifier, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if imports[qualifier.Name] == "github.com/division-sh/swarm/internal/store" && (selector.Sel.Name == "PostgresStore" || selector.Sel.Name == "SQLiteRuntimeStore") && (insideServe || (!strings.HasSuffix(path, "_test.go") && !insideStore && !insideInfrastructure)) {
				add(name, "concrete:"+selector.Sel.Name, "concrete selected-store recovery/projection")
			}
			return true
		})
	}
	return findings
}

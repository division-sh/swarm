package store_test

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the collector payload to avoid a self-referential fingerprint.
const debtProcessLeaseCollectorTo = "1a353f6c8e5cfe2e2fce2163231e4ac4832df0776daa8dbea177c6edb274bd90"
const debtProcessLeaseFreshnessCollectorTo = "b06e27c4770377190d13b3218ff93830312175c35e1764cf331f8f61e2309e75"

const debtR04Before = "af47497e27bcc0cca593fb56f5767ff68f201ce1"
const debtR04After = "b24a0421fa4ffe29f8e89f31fe54720996b0d278"
const debtR04CustodyTo = "d2212c6e9f90807448b5ee61f2781aee485bf78c28a84a338e9b43fd2e9a142b"
const debtR04CollectorTo = "34691798f6437c504ebc6ef1c3cb359bccd0d368a515456f4aeb42d42c3084b0"
const debtR04OwnerPath = "internal/store/internal/startupownership/owner.go"
const debtR04ActivityPath = "internal/store/internal/startupownership/activity_recovery.go"
const debtR04CompositionPath = "internal/store/internal/runtimepersistence/composition.go"
const debtR04NativeRepairAfter = "a88c60be70f6923ef7a3960aa12dcbf2e124e06c"
const debtR04NativeCustodyTo = "ec1fec1bdff3d92cf2de9f414fff6cc5c9a74feb8138313b8bb4e92e246ab3fa"
const debtR04NativeCollectorTo = "c79af9700325a028f169ec3c2284828751f97be022233cfd6356f1c726e4ce3d"

// #2700/6249776750: append the approved complexity extractions; old edges stay.
const debtR04ExtractionBefore = "70fa4d34de038b16989570a200dd5b8bcdf34d2b"
const debtR04ExtractionAfter = "5a225842dd670525185b066abea89eb86f99d1c6"
const debtR04ExtractionCollectorTo = "41e687f6b2655593235acdfe08c19a02cda85c9ee13c5f4ff80eeebe83ff7dca"

var debtR04ExtractionPins = [...]debtR04CatchUpSource{
	{
		File:         "internal/store/internal/backend/activityjournal/activity_execution_grant.go",
		BeforeSHA256: "00e81af8c72afe78c7097610ca7ce5fde612091edb270edb4729185977220925",
		AfterSHA256:  "b0922cbc825c3401dedd2322212bd598a6e43c8d0c6e504f3eccad5e7976a0a0",
		Changed:      []string{"func:ReadActivityExecutionGrantTx"},
		Added:        []string{"type:activityExecutionGrantRow", "func:decodeActivityExecutionGrantRow"},
	},
	{
		File:         "internal/store/internal/backend/effectpersistence/provider_resume_admission.go",
		BeforeSHA256: "9859da908a42a54d26cfa628e5bdb2e5885996076f445f8b8059c32cd813e3c8",
		AfterSHA256:  "3b845e79dac06e96e9fd352c05780bbb5f04eadabc841a36af83226122644f66",
		Changed:      []string{"func:authorizeProviderResumeTx"},
		Added:        []string{"func:providerResumeEvidenceTx"},
	},
	{
		File:         "internal/store/internal/backend/effectpersistence/completion_recovery.go",
		BeforeSHA256: "0296d328ba4a1345eb86779d9975ecb75480ac564ebbcea81d199329d1e7b520",
		AfterSHA256:  "46f0b975692b0ca804d889daed5191c184b6c36bda654fa2b50f1da89fdea4fb",
		Changed:      []string{"func:completionRecoverySettlement"},
		Added:        []string{"func:completionRecoveryExecutionAuthority", "func:completionRecoveryAgentTurn"},
	},
	{
		File:         "internal/store/internal/backend/postgres/transaction.go",
		BeforeSHA256: "604eab38ee4d7c4ad899a4b82662320ea3a7070be41a92da0277309de5d02197",
		AfterSHA256:  "2270b20fcfe84f45402f7cd59a8b7364394bbc2e898a69dd9434697b242571e1",
		Changed:      []string{"func:(*Backend).runTransactionOutcome"},
		Added:        []string{"func:rollbackAndCloseTransactionConnection", "func:closeTransactionConnection"},
	},
}

var debtR04NativeRepairPins = [...]debtRepair2698SourceFile{
	{"internal/store/internal/backend/activityjournal/activity_execution_grant.go", "absent", "00e81af8c72afe78c7097610ca7ce5fde612091edb270edb4729185977220925"},
	{"internal/store/internal/backend/activityjournal/displaced_recovery.go", "13a9f50ced01b7043756f79e81ea40be25e728a7cd6b849c2a43e22ffde6294f", "absent"},
	{"internal/store/internal/backend/activityjournal/execution_grant.go", "ab720d379d250a2dfb764c5d7d876b6f6778798c99dadd7882851cc99722763d", "d1e8d847374ef53d35d4c7b43fbe1a41f4a2f83625a6ef563bbb9d428c649968"},
	{"internal/store/internal/backend/activityjournal/recovery_binding.go", "absent", "2d394f12068f129fb4f0676124d866dd999b0d120db409e4b25f8b982a729317"},
	{"internal/store/internal/startupownership/activity_execution_grant.go", "ead99b547ad30fb9ef5776f1281d3d2c146757d3e2ceeb7652b4d38337f7b12f", "absent"},
	{"internal/store/internal/startupownership/activity_recovery.go", "3cc9b6af3c7bdf6b5664c88bd6b0f5cfdb0e36891eae5c5944a1fc4b4bf5e91e", "cef83d1ec303eecadd52afd40fad49f9dfcfd99dbb74b4fad3ca32cffbc38bc8"},
	{"internal/store/internal/startupownership/activity_recovery_native.go", "absent", "f992d066041b3161a11832afe5754cec2f97c5bb8acc12d2f12c8db461ab6e49"},
	{"internal/store/internal/startupownership/displaced_http_activity.go", "51b6d4c5e1b341a898b0a5b9010ea4abc63234af130ee220d4f6106dc2f4a693", "12abf02bddb116f976d3fe29da263bf35b0a61e6ae1eab4a2d7afdefa6e8dec7"},
	{"internal/store/internal/startupownership/owner.go", "b189576ad1095fdf2d4d25e73e7baa536333084fad4b289853cc2fdabdf68f68", "6cb13742409e290fce5e5d62177b4dd79b7882cd267b4cafcc5bfde1399ef355"},
}

var debtR04SourcePins = [...]debtRepair2698SourceFile{
	{debtR04OwnerPath, "efa6f487608ad86229e863bb8717c98d92745ea25757696346c66da07540e4d7", "b189576ad1095fdf2d4d25e73e7baa536333084fad4b289853cc2fdabdf68f68"},
	{debtR04ActivityPath, "absent", "3cc9b6af3c7bdf6b5664c88bd6b0f5cfdb0e36891eae5c5944a1fc4b4bf5e91e"},
	{debtR04CompositionPath, "1ff439f97646d8df6a4e9d333a4f3b581902278d7e8233fe92f33bb72c01ea36", "9a388b421d837cd0ee7432819eff051444727ce70040cb444cfaf1250d74d021"},
}

func TestPersistenceAuthorityDebtProcessLeaseReviewedSourcePins(t *testing.T) {
	root, base := persistenceAuthorityRepoRoot(t), t.TempDir()
	debtRequireProcessLeasePredecessor(t, root)
	for _, path := range []string{"internal/store/construction/open.go", "internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go"} {
		data, err := debtGit(root, "show", "f8ffc42420a93b6dfb3575c4a3dddeee9d858693:"+path)
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, base, path, string(data))
	}
	for i, pair := range debtProcessLeaseRelocations() {
		for j, tree := range []string{base, root} {
			digest, err := debtProcessLeaseDeclarationDigest(tree, pair[j].File, pair[j].Declaration)
			if err != nil || digest != debtProcessLeaseSourcePins[i][j] {
				t.Errorf("pin[%d][%d]=%q error=%v", i, j, digest, err)
			}
		}
	}
	if digest, err := debtProcessLeaseCustodyDigest(root); err != nil || debtR04CustodySource(root, digest) != nil {
		t.Errorf("custody pin=%q error=%v", digest, err)
	}
	proveR04SourceContinuation(t, root)
	proveR04HistoricalSourceContinuation(t, root)
	if digest, err := debtCollectorDigest(debtCatchUpHistoricalArchive(t, debtCatchUpMaster)); err != nil || digest != debtProcessLeaseCollectorTo {
		t.Errorf("collector pin=%q error=%v", digest, err)
	}
	if digest, err := debtCollectorDigest(debtCatchUpHistoricalArchive(t, "a2621ebde98d9a43d9783cbfd078b63ccc3ab14a")); err != nil || digest != debtProcessLeaseFreshnessCollectorTo {
		t.Errorf("freshness collector pin=%q error=%v", digest, err)
	}
}

func TestPersistenceAuthorityDebtProcessLeaseRelocationsAreOneWay(t *testing.T) {
	for _, pair := range debtProcessLeaseRelocations() {
		old, current := pair[0], pair[1]
		t.Run(current.Declaration, func(t *testing.T) {
			before, after := debtControlSet(old), debtControlSet(current)
			for _, head := range []map[string]authorityDebtSite{before, after} {
				if failures := authorityDebtRatchet(after, head, before, before); len(failures) != 0 {
					t.Fatal(failures)
				}
			}
			if failures := authorityDebtRatchet(after, after, after, after); len(failures) != 0 {
				t.Fatal("unchanged landed successor rejected:", failures)
			}
			if failures := authorityDebtRatchet(debtControlSet(), after, after, after); len(failures) != 0 {
				t.Fatal("genuine later deletion rejected:", failures)
			}
			_, normalized, source, failures := debtProcessLeaseRelocationComparisons(after, before, before, before)
			if len(failures) != 0 || authorityDebtCount(normalized) != old.Multiplicity || authorityDebtCount(source) != old.Multiplicity {
				t.Fatal("relocation earned deletion credit")
			}
			for _, mutation := range []string{"missing-donor", "changed-donor", "source-drift", "reverse", "increase", "coexist-actual", "coexist-head", "coexist-base", "coexist-source", "foreign-file", "foreign-type", "foreign-owner", "foreign-operation", "foreign-declaration", "unrelated-addition"} {
				t.Run(mutation, func(t *testing.T) {
					actual, head, trusted, origin := maps.Clone(after), maps.Clone(after), maps.Clone(before), maps.Clone(before)
					changed := current
					delete(actual, current.identity())
					switch mutation {
					case "missing-donor":
						trusted = debtControlSet()
					case "changed-donor":
						donor := old
						donor.Multiplicity++
						trusted = debtControlSet(donor)
					case "source-drift":
						origin = debtControlSet()
					case "reverse":
						actual, head, trusted, origin = before, before, after, after
					case "increase":
						changed.Multiplicity++
					case "coexist-actual":
						actual[old.identity()] = old
					case "coexist-head":
						head[old.identity()] = old
					case "coexist-base":
						trusted[current.identity()] = current
					case "coexist-source":
						origin[current.identity()] = current
					case "foreign-file":
						changed.File = "internal/runtime/foreign.go"
					case "foreign-type":
						changed.Resolved += "foreign"
					case "foreign-owner":
						changed.Replacement += "foreign"
					case "foreign-operation":
						changed.Operation += "foreign"
					case "foreign-declaration":
						changed.Declaration += "foreign"
					case "unrelated-addition":
						added := debtControlSite("call:Exec", 1)
						actual[added.identity()] = added
					}
					if mutation != "reverse" {
						actual[changed.identity()] = changed
					}
					if failures := authorityDebtRatchet(actual, head, trusted, origin); len(failures) == 0 {
						t.Fatal("hostile relocation admitted")
					}
				})
			}
		})
	}
}

func TestPersistenceAuthorityDebtProcessLeaseCollectorTransitionIsBounded(t *testing.T) {
	for _, donor := range []string{debtForkProofCollectorTo} {
		t.Run(donor[:8], func(t *testing.T) {
			before, after := debtControlSet(), debtControlSet()
			for _, pair := range debtProcessLeaseRelocations() {
				before[pair[0].identity()], after[pair[1].identity()] = pair[0], pair[1]
			}
			inherited := debtControlSite("call:QueryRow", 2)
			before[inherited.identity()], after[inherited.identity()] = inherited, inherited
			trusted := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: donor, Sites: before}
			head := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: debtProcessLeaseCollectorTo, Sites: after}
			for _, mutation := range []string{"exact", "downward", "landed-successor", "reverse", "unlanded-role-predecessor", "foreign-base", "foreign-destination", "unrotated", "rebootstrap", "missing-donor", "addition", "increase"} {
				t.Run(mutation, func(t *testing.T) {
					base, candidate := trusted, head
					base.Sites, candidate.Sites = maps.Clone(trusted.Sites), maps.Clone(head.Sites)
					current := debtProcessLeaseCollectorTo
					switch mutation {
					case "downward":
						delete(candidate.Sites, inherited.identity())
					case "landed-successor":
						base = candidate
					case "reverse":
						base, candidate, current = candidate, base, base.Collector
					case "unlanded-role-predecessor":
						base.Collector = debtFixtureRoleCollectorTo
					case "foreign-base":
						base.Collector = strings.Repeat("a", 64)
					case "foreign-destination":
						candidate.Collector, current = strings.Repeat("b", 64), strings.Repeat("b", 64)
					case "unrotated":
						candidate.Collector = base.Collector
					case "rebootstrap":
						candidate.BootstrapSource = strings.Repeat("c", 40)
					case "missing-donor":
						delete(base.Sites, debtProcessLeaseRelocations()[0][0].identity())
					case "addition":
						added := debtControlSite("call:Exec", 1)
						candidate.Sites[added.identity()] = added
					case "increase":
						inherited.Multiplicity++
						candidate.Sites[inherited.identity()] = inherited
					}
					accept := mutation == "exact" || mutation == "downward" || mutation == "landed-successor"
					if err := debtValidateCollectorIdentity(base.Collector, current, base, candidate, ""); (err == nil) != accept {
						t.Fatalf("accept=%v error=%v", accept, err)
					}
				})
			}
		})
	}
	t.Run("freshness-metadata", func(t *testing.T) {
		inherited := debtControlSite("call:QueryRow", 2)
		trusted := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: debtProcessLeaseCollectorTo, Sites: debtControlSet(inherited)}
		head := trusted
		head.Collector = debtProcessLeaseFreshnessCollectorTo
		for _, mutation := range []string{"exact", "landed-successor", "reverse", "wrong-base", "wrong-trusted", "unmatched-base", "wrong-destination", "wrong-head", "unmatched-current", "rebootstrap", "addition", "increase", "deletion"} {
			t.Run(mutation, func(t *testing.T) {
				base, candidate := trusted, head
				base.Sites, candidate.Sites = maps.Clone(trusted.Sites), maps.Clone(head.Sites)
				predecessor, current := base.Collector, candidate.Collector
				switch mutation {
				case "landed-successor":
					base, predecessor = candidate, current
				case "reverse":
					base, candidate, predecessor, current = candidate, base, current, predecessor
				case "wrong-base":
					predecessor, base.Collector = strings.Repeat("a", 64), strings.Repeat("a", 64)
				case "wrong-trusted":
					base.Collector = strings.Repeat("a", 64)
				case "unmatched-base":
					predecessor = strings.Repeat("a", 64)
				case "wrong-destination":
					current, candidate.Collector = strings.Repeat("b", 64), strings.Repeat("b", 64)
				case "wrong-head":
					candidate.Collector = strings.Repeat("b", 64)
				case "unmatched-current":
					current = strings.Repeat("b", 64)
				case "rebootstrap":
					candidate.BootstrapSource = strings.Repeat("c", 40)
				case "addition":
					added := debtControlSite("call:Exec", 1)
					candidate.Sites[added.identity()] = added
				case "increase":
					increased := inherited
					increased.Multiplicity++
					candidate.Sites[increased.identity()] = increased
				case "deletion":
					delete(candidate.Sites, inherited.identity())
				}
				accept := mutation == "exact" || mutation == "landed-successor"
				if err := debtValidateCollectorIdentity(predecessor, current, base, candidate, ""); (err == nil) != accept {
					t.Fatalf("accept=%v error=%v", accept, err)
				}
			})
		}
		root := debtCatchUpHistoricalArchive(t, "a2621ebde98d9a43d9783cbfd078b63ccc3ab14a")
		baseline, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := os.ReadFile(filepath.Join(root, debtCollectorSidecarPath))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := debtParseCollectorMetadata(metadata, baseline, debtProcessLeaseFreshnessCollectorTo); err != nil {
			t.Fatal("exact current sidecar rejected:", err)
		}
		if _, err := debtParseCollectorMetadata(metadata, baseline, debtProcessLeaseCollectorTo); err == nil {
			t.Fatal("stale source digest admitted by current sidecar")
		}
		stale, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtProcessLeaseCollectorTo, BaselineSHA256: debtBaselineChecksum(baseline), Predecessor: debtCacheCollectorFrom})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := debtParseCollectorMetadata(stale, baseline, debtProcessLeaseFreshnessCollectorTo); err == nil {
			t.Fatal("mismatched sidecar admitted by current source")
		}
	})
}

func TestPersistenceAuthorityDebtProcessLeaseRelocationRejectsSourceDrift(t *testing.T) {
	root, base, changed := persistenceAuthorityRepoRoot(t), t.TempDir(), t.TempDir()
	debtRequireProcessLeasePredecessor(t, root)
	before, after := debtControlSet(), debtControlSet()
	for _, pair := range debtProcessLeaseRelocations() {
		before[pair[0].identity()], after[pair[1].identity()] = pair[0], pair[1]
	}
	for _, path := range []string{"internal/store/construction/open.go", "internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go"} {
		old, err := debtGit(root, "show", "f8ffc42420a93b6dfb3575c4a3dddeee9d858693:"+path)
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, base, path, string(old))
		current, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, changed, path, string(current))
	}
	if err := debtValidateProcessLeaseRelocationSource(root, base, after, before); err != nil {
		t.Fatal(err)
	}
	const path = "internal/store/construction/open.go"
	data, err := os.ReadFile(filepath.Join(changed, path))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, changed, path, strings.Replace(string(data), "private.ComposeSQLiteRuntimeStoreWithBackendIdentity(schema, backend, identity)", "private.ComposeSQLiteRuntimeStore(schema, backend)", 1))
	if err := debtValidateProcessLeaseRelocationSource(changed, base, after, before); err == nil {
		t.Fatal("changed native constructor provenance admitted")
	}
	if err := debtValidateProcessLeaseRelocationSource(root, changed, after, before); err == nil {
		t.Fatal("changed donor source admitted")
	}
}

func debtRequireProcessLeasePredecessor(t *testing.T, root string) {
	t.Helper()
	if _, err := debtGit(root, "cat-file", "-e", "f8ffc42420a93b6dfb3575c4a3dddeee9d858693^{commit}"); err != nil {
		// Use the existing authenticated lineage owner to deepen hosted shallow
		// checkouts. Never accept caller-selected source or a baseline file.
		if _, err := debtTrustedBase(root); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPersistenceAuthorityDebtUnsupportedProcessWaitsUseTypedRefusal(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	const path = "internal/store/internal/startupownership/sqlite_possession_unsupported.go"
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n\t_, err := acquireSQLiteFilePossession(\"\")\n\treturn nil, err\n}"
	seen := map[string]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || (fn.Name.Name != "waitSQLiteFilePossession" && fn.Name.Name != "waitSQLiteConstructionPossession") {
			continue
		}
		var body bytes.Buffer
		if err := format.Node(&body, token.NewFileSet(), fn.Body); err != nil {
			t.Fatal(err)
		}
		if body.String() != want {
			t.Fatalf("%s differs from the exact existing typed-refusal forwarder: %s", fn.Name, &body)
		}
		seen[fn.Name.Name] = true
	}
	if len(seen) != 2 {
		t.Fatal("both unsupported process waits must retain the refusal")
	}
}

func TestPersistenceAuthorityDebtProcessLeaseCustodyIsSourceBound(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	const dir = "internal/store/internal/startupownership"
	entries, err := os.ReadDir(filepath.Join(root, dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"actual", "raw-field", "raw-getter", "sql-callback", "forwarder", "custody-change", "construction-wiring", "r04-role", "r04-missing-role", "r04-binding", "r04-duplicate", "r04-missing-assembly", "r04-sqlite-field", "r04-source-value-escape", "r04-warm-role"} {
		t.Run(mutation, func(t *testing.T) {
			copy := t.TempDir()
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
					continue
				}
				data, err := os.ReadFile(filepath.Join(root, dir, entry.Name()))
				if err != nil {
					t.Fatal(err)
				}
				text := string(data)
				if entry.Name() == "owner.go" && mutation == "raw-field" {
					text = strings.ReplaceAll(text, "constructionLease", "ConstructionLease")
				}
				if entry.Name() == "postgres_construction.go" && mutation == "custody-change" {
					text = strings.Replace(text, "return lease, capacity, nil", "return s.acquireConstructionPossession(ctx)", 1)
				}
				if entry.Name() == "postgres_construction.go" && mutation == "construction-wiring" {
					text = strings.Replace(text, "schemastore.BindPostgresProcessLease(schema, lease)", "schemastore.BindPostgresProcessLease(schema, nil)", 1)
				}
				if entry.Name() == "activity_recovery.go" && mutation == "r04-role" {
					text = strings.Replace(text, "s.activityJournal = role", "s.activityJournal = nil", 1)
				}
				if entry.Name() == "owner.go" && mutation == "r04-sqlite-field" {
					text = strings.Replace(text, "lifecycleCandidates *storerunhandoff.CandidateCoordinator", "lifecycleCandidates any", 1)
				}
				if entry.Name() == "activity_recovery.go" && mutation == "r04-missing-role" {
					continue
				}
				debtWriteModuleSource(t, copy, dir+"/"+entry.Name(), text)
			}
			copyR04CompositionSource(t, root, copy)
			if mutation == "r04-warm-role" {
				if _, _, err := debtR04SourceImages(copy); err != nil {
					t.Fatal("complete R04 copy failed before warming:", err)
				}
				data, err := os.ReadFile(filepath.Join(copy, debtR04ActivityPath))
				if err != nil {
					t.Fatal(err)
				}
				debtWriteModuleSource(t, copy, debtR04ActivityPath, strings.Replace(string(data), "s.activityJournal = role", "s.activityJournal = nil", 1))
			}
			if mutation == "r04-binding" {
				data, err := os.ReadFile(filepath.Join(copy, debtR04CompositionPath))
				if err != nil {
					t.Fatal(err)
				}
				debtWriteModuleSource(t, copy, debtR04CompositionPath, strings.Replace(string(data), "startupOwner.BindActivityJournalRole(activityJournal)", "startupOwner.BindActivityJournalRole(nil)", 1))
			}
			if mutation == "r04-missing-assembly" {
				if err := os.Remove(filepath.Join(copy, debtR04CompositionPath)); err != nil {
					t.Fatal(err)
				}
			}
			if mutation == "r04-source-value-escape" {
				data, err := os.ReadFile(filepath.Join(copy, debtR04OwnerPath))
				if err != nil {
					t.Fatal(err)
				}
				debtWriteModuleSource(t, copy, debtR04OwnerPath, string(data)+"\nvar leakedR04Role = (*StartupSQLiteOwner).BindActivityJournalRole\n")
			}
			additions := map[string]string{
				"raw-getter":    "func(s *StartupPostgresOwner)Raw()any{return s.constructionLease}",
				"sql-callback":  "func(s *StartupPostgresOwner)Call(f func(any)){f(s.constructionLease)}",
				"forwarder":     "func(s *StartupPostgresOwner)Forward()any{return s.Raw()}",
				"r04-duplicate": "func(s *StartupPostgresOwner)BindActivityJournalRole(role displacedHTTPActivityJournal)error{return nil}",
			}
			if extra := additions[mutation]; extra != "" {
				debtWriteModuleSource(t, copy, dir+"/escape.go", "package startupownership\n"+extra)
			}
			const typ = "github.com/division-sh/swarm/internal/store/internal/startupownership.StartupPostgresOwner"
			findings := []authorityFinding{
				{Kind: "raw-authority-export", File: dir + "/owner.go", Enclosing: "StartupPostgresOwner", Member: "exported-type", Resolved: typ, RawSQL: true},
				{Kind: "raw-authority-export", File: dir + "/owner.go", Enclosing: "NewPostgres", Member: "result:#1", Resolved: "*" + typ, RawSQL: true},
			}
			debtClassifyProcessLeaseCustody(copy, findings)
			proveR04JournalBindingCustody(t, copy, mutation == "actual")
			for _, finding := range findings {
				if allowed := debtRawSQLDispositionAllowed(finding, "fixture-2151"); allowed != (mutation == "actual") {
					t.Fatalf("mutation=%s owned=%v finding=%+v", mutation, allowed, finding)
				}
				if mutation == "actual" {
					for _, field := range []string{"kind", "file", "declaration", "member", "resolved", "raw"} {
						foreign := finding
						mutate := map[string]*string{"kind": &foreign.Kind, "file": &foreign.File, "declaration": &foreign.Enclosing, "member": &foreign.Member, "resolved": &foreign.Resolved}
						if field == "raw" {
							foreign.RawSQL = false
							if debtProcessLeaseCustodyFinding(foreign) {
								t.Fatal("nonraw identity misclassified")
							}
							continue
						}
						*mutate[field] += "foreign"
						if debtProcessLeaseCustodyFinding(foreign) || (field != "kind" && debtRawSQLDispositionAllowed(foreign, "fixture-2151")) {
							t.Fatalf("foreign %s received native custody class", field)
						}
					}
				}
			}
		})
	}
}

func proveR04JournalBindingCustody(t *testing.T, root string, accept bool) {
	t.Helper()
	for _, backend := range []string{"Postgres", "SQLite"} {
		const prefix = "github.com/division-sh/swarm/internal/store/internal/"
		binding := authorityFinding{Kind: "effective-method", RawSQL: true,
			File: "internal/store/internal/startupownership/<effective-method-set>", Enclosing: prefix + "startupownership.Startup" + backend + "Owner",
			Member: "method:BindActivityJournalRole", Resolved: "func(role *" + prefix + "backend/activityjournal.Activity" + backend + "Owner) error"}
		if r04JournalBindingConstructionCustody(root, binding) != accept {
			t.Fatal("original exported journal role lost exact source custody:", backend, accept)
		}
		if !accept {
			continue
		}
		for _, field := range []string{"kind", "file", "owner", "member", "signature"} {
			foreign := binding
			parts := map[string]*string{"kind": &foreign.Kind, "file": &foreign.File, "owner": &foreign.Enclosing, "member": &foreign.Member, "signature": &foreign.Resolved}
			*parts[field] += "foreign"
			if r04JournalBindingConstructionCustody(root, foreign) {
				t.Fatal("foreign journal custody admitted:", field)
			}
		}
	}
}

func TestPersistenceAuthorityDebtProcessLeaseCustodyUsesCollectorMember(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	const path = "internal/store/internal/startupownership/owner.go"
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	const typ = "github.com/division-sh/swarm/internal/store/internal/startupownership.StartupPostgresOwner"
	var findings []authorityFinding
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != "NewPostgres" {
			continue
		}
		// Use the collector's actual authored-field naming, not a shortened TSV
		// operation (which discards the occurrence index).
		findings = append(findings, authorityFinding{Kind: "raw-authority-export", File: path, Enclosing: fn.Name.Name,
			Member: debtAuthorityFieldName("result", 0, fn.Type.Results.List[0]), Resolved: "*" + typ, RawSQL: true})
	}
	findings = append(findings, authorityFinding{Kind: "raw-authority-export", File: path, Enclosing: "StartupPostgresOwner",
		Member: "exported-type", Resolved: typ, RawSQL: true})
	debtClassifyProcessLeaseCustody(root, findings)
	if len(findings) != 2 || len(authorityDebtSites(findings)) != 0 {
		t.Fatalf("exact source-owned findings must remain visible but not become consumer debt: %+v", findings)
	}
	for _, finding := range findings {
		if !finding.RawSQL || finding.Kind != "raw-authority-export" || !debtProcessLeaseCustodyFinding(finding) {
			t.Fatal("classification hid or altered the all-owner evidence:", finding)
		}
	}
}

package bus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestMixedPubsubConnectAuthorityStructuralGuard(t *testing.T) {
	for owner, retired := range map[reflect.Type][]string{
		reflect.TypeOf(RouteTable{}):           {"templateObservers"},
		reflect.TypeOf(routePattern{}):         {"SourceInstancePath"},
		reflect.TypeOf(routeResolvedPattern{}): {"SourceTemplatePath", "SourceLocalEvent"},
	} {
		for _, name := range retired {
			if _, present := owner.FieldByName(name); present {
				t.Errorf("compiled %s retains retired observer authority %s", owner, name)
			}
		}
	}
	files := parseBusProductionFiles(t)
	requiredCalls := map[string]map[string]bool{
		"appendUniqueSubscriber": {
			"resolvedSubscriberRoleKey": false,
		},
		"dedupeSubscribers": {
			"resolvedSubscriberRoleKey": false,
		},
		"deliveryPlanner.planRecipients": {
			"planIndependentPubsubBranch":    false,
			"composeIndependentPubsubBranch": false,
		},
		"resolveRoutePlanEventProjection": {
			"DeliveryRoutes":                              false,
			"preparedEventTargetProjectionForRoutes":      false,
			"ResolvePreparedPublishEventTargetProjection": false,
		},
		"ResolvePreparedPublishEventTargetProjection": {
			"preparedEventTargetProjectionForRoutes": false,
			"Envelope":                               false,
		},
		"preparedEventTargetProjection.Envelope": {
			"EnvelopeForTargetRoute": false,
			"EnvelopeForTargetSet":   false,
		},
	}
	forbiddenIdentifiers := map[string]struct{}{
		"templateObservers":                        {},
		"routeTemplateSourceObserver":              {},
		"addTemplateSourceObserverLocked":          {},
		"materializeTemplateSourceObserversLocked": {},
		"materializeTemplateSourceObserverLocked":  {},
		"compiledRouteOwnerDependencies":           {},
		"pubsubObserverDeclarations":               {},
		"SourceTemplatePath":                       {},
		"SourceLocalEvent":                         {},
		"SourceInstancePath":                       {},
		"EventDeliveryTargetReader":                {},
		"DeliveryTargets":                          {},
		"ListEventDeliveryTargets":                 {},
		"deliveryTargetsForManifest":               {},
		"deliveryTargetsForEvent":                  {},
		"cloneRouteTargetMap":                      {},
	}

	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			if _, forbidden := forbiddenIdentifiers[identifier.Name]; forbidden {
				t.Errorf("%s: retired recipient-ID target authority %s returned", path, identifier.Name)
			}
			return true
		})

		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			key := receiverFunctionKey(function)
			expected, guarded := requiredCalls[key]
			if !guarded {
				expected, guarded = requiredCalls[function.Name.Name]
				key = function.Name.Name
			}
			if !guarded {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if _, required := expected[calledFunctionName(call)]; required {
					expected[calledFunctionName(call)] = true
				}
				return true
			})
			requiredCalls[key] = expected
		}
	}

	for function, calls := range requiredCalls {
		for call, found := range calls {
			if !found {
				t.Errorf("mixed routing authority guard lost %s consumption in %s", call, function)
			}
		}
	}
}

func parseBusProductionFiles(t testing.TB) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read bus package: %v", err)
	}
	files := make(map[string]*ast.File)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files[name] = file
	}
	return files
}

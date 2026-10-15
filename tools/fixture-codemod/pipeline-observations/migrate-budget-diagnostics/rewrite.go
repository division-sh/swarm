package main

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

func transformSource(original, current []byte) ([]byte, []runphysical.Pin) {
	names := []string{"TestChannelDeliveryBudgetNoticePublicJourney", "logBudgetNoticeFailure"}
	var pins []runphysical.Pin
	for _, name := range names {
		before := declaration(original, name)
		var after string
		if name == "logBudgetNoticeFailure" {
			after = rewriteLogger(before)
		} else {
			after = rewriteJourney(before)
		}
		after, err := requestkey.Normalize(after)
		must(err)
		pins = append(pins, pin(name, before, after))
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, sourcePath, current, parser.AllErrors)
	must(err)
	result := string(current)
	seen := map[string]int{}
	for i := len(file.Decls) - 1; i >= 0; i-- {
		fn, ok := file.Decls[i].(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		for _, row := range pins {
			if row.Function != fn.Name.Name {
				continue
			}
			seen[row.Function]++
			body := requestkey.Node(fn)
			if body != row.Before && body != row.After {
				panic("budget caller changed outside its exact signed rule: " + row.Function)
			}
			start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
			result = result[:start] + row.After + result[end:]
		}
	}
	for _, row := range pins {
		if seen[row.Function] != 1 {
			panic("budget finite declaration missing or duplicated: " + row.Function)
		}
	}
	const importLine = "\"github.com/division-sh/swarm/internal/store/storetest\""
	if !strings.Contains(result, importLine) {
		result = replace(result, "\"github.com/division-sh/swarm/internal/servedparity\"", "\"github.com/division-sh/swarm/internal/servedparity\"\n"+importLine)
	}
	raw, err := format.Source([]byte(result))
	must(err)
	return raw, pins
}

func rewriteJourney(before string) string {
	start := strings.Index(before, "\n\t\t\tdriver := \"sqlite\"")
	end := strings.Index(before, "\n\t\t\tdeadline := time.Now().Add(20 * time.Second)")
	if start < 0 || end < start {
		panic("budget observation's original pool lifetime changed")
	}
	after := before[:start] + "\n\t\t\treader := channelBudgetStorageObservation(t, h)" + before[end:]
	queries := sqlLiterals(before)
	if len(queries) != 2 {
		panic("budget positive query inventory changed")
	}
	old := "db.QueryRow(`" + queries[0] + "`).Scan(&noticeID, &entity, &priority, &producer, &summary, &eventName)"
	new := `func() error { out,err:=reader.ReadChannelBudgetEmergencyNoticeStorage(context.Background());noticeID,entity,priority,producer,summary,eventName=out.ItemID,out.EntityID,out.Severity,out.FromAgent,out.Summary,out.EventName;return err }()`
	after = replace(after, old, new)
	after = replace(after, "logBudgetNoticeFailure(t, db,", "logBudgetNoticeFailure(t, reader,")
	old = "db.QueryRow(`" + queries[1] + "`).Scan(&completions)"
	new = `func() error { value,err:=reader.CountChannelBudgetLiveCompletionsStorage(context.Background());completions=value;return err }()`
	after = replace(after, old, new)
	end = strings.LastIndex(after, "\n\t\t})")
	if end < 0 {
		panic("budget journey backend closure disappeared")
	}
	return after[:end] + "\n\t\t\trequireChannelBudgetDiagnosticManagedEvidence(t, reader)" + after[end:]
}

func rewriteLogger(before string) string {
	after := replace(before, "db *sql.DB", "reader storetest.ChannelBudgetObservation")
	start := strings.Index(after, "\n\tfor _, query := range")
	if start < 0 {
		panic("budget diagnostic's original ordered loop disappeared")
	}
	after = after[:start] + "\nfor _,query:=range []struct{name string;read func(context.Context)([]map[string]string,error)}{\n"
	names := []string{"spend", "effects", "agents", "deliveries", "budget_events", "notices"}
	for i, projection := range projections {
		after += fmt.Sprintf("{%q,reader.ReadChannelBudget%sDiagnosticsStorage},\n", names[i], projection.Name)
	}
	return after + `}{rows,err:=query.read(ctx);t.Logf("budget notice diagnostic %s=%v err=%v",query.name,rows,err)}
}`
}

func replace(source, before, after string) string {
	if strings.Count(source, before) != 1 {
		panic("budget finite rewrite has no unique source cut: " + before)
	}
	return strings.Replace(source, before, after, 1)
}

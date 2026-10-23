package schemastore

import (
	"database/sql"
	"fmt"
	"testing"
)

type forkLiveParentFixture struct {
	id, binding, child, source, kind string
	revision, generation             int64
	event                            sql.NullString
}

func verifyForkLiveParentPairs(t *testing.T, db *sql.DB, sourceEventID, otherEventID string) {
	t.Helper()
	for _, execution := range []bool{false, true} {
		canonical, parents, idColumn, bindingColumn := "run_fork_operations", "run_fork_operation_live_parents", "operation_id", "selected_binding_id"
		generation := "1"
		if execution {
			canonical, parents, idColumn, bindingColumn = "run_fork_selected_contract_runtime_executions", "run_fork_selected_contract_execution_live_parents", "execution_id", "binding_id"
			generation = "generation"
		}
		rows, err := db.Query(fmt.Sprintf(`SELECT %s,%s,fork_run_id,source_run_id,fork_point_kind,fork_revision,fork_event_id,%s FROM %s ORDER BY fork_point_kind`, idColumn, bindingColumn, generation, canonical))
		if err != nil {
			t.Fatal(err)
		}
		var facts []forkLiveParentFixture
		for rows.Next() {
			var fact forkLiveParentFixture
			if err := rows.Scan(&fact.id, &fact.binding, &fact.child, &fact.source, &fact.kind, &fact.revision, &fact.event, &fact.generation); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			facts = append(facts, fact)
		}
		readErr := rows.Err()
		closeErr := rows.Close()
		if readErr != nil || closeErr != nil || len(facts) != 3 || facts[0].kind != "deployment_revision" || facts[1].kind != "event" || facts[2].kind != "state_revision" {
			t.Fatalf("read exact event/deployment/state canonical facts: rows=%d read=%v close=%v", len(facts), readErr, closeErr)
		}
		columns := idColumn + "," + bindingColumn + ",fork_run_id,source_run_id,fork_point_kind,fork_revision,fork_event_id"
		values := "$1,$2,$3,$4,$5,$6,$7"
		if execution {
			columns += ",generation"
			values += ",$8"
		}
		insert := "INSERT INTO " + parents + " (" + columns + ") VALUES (" + values + ")"
		for index, fact := range facts {
			t.Run(parents+"/"+fact.kind, func(t *testing.T) {
				args := []any{fact.id, fact.binding, fact.child, fact.source, fact.kind, fact.revision, nil}
				if fact.event.Valid {
					args[6] = fact.event.String
				}
				if execution {
					args = append(args, fact.generation)
				}
				if _, err := db.Exec(insert, args...); err != nil {
					t.Fatalf("exact canonical live pair refused: %v", err)
				}
				if _, err := db.Exec("DELETE FROM "+parents+" WHERE "+idColumn+"=$1", fact.id); err != nil {
					t.Fatal(err)
				}
				for _, fault := range []string{"missing_owner", "missing_kind", "crossed_binding", "crossed_child", "crossed_source", "changed_revision", "wrong_event", "false_null_event", "false_null_kind", "changed_generation"} {
					if fault == "changed_generation" && !execution {
						continue
					}
					t.Run(fault, func(t *testing.T) {
						changed := append([]any(nil), args...)
						switch fault {
						case "missing_owner":
							changed[0] = nil
						case "missing_kind":
							changed[4] = nil
						case "crossed_binding":
							changed[1] = facts[(index+1)%len(facts)].binding
						case "crossed_child":
							changed[2] = fact.source
						case "crossed_source":
							changed[3] = fact.child
						case "changed_revision":
							changed[5] = fact.revision + 1
						case "wrong_event":
							changed[6] = otherEventID
						case "false_null_event":
							if fact.event.Valid {
								changed[6] = nil
							} else {
								changed[4] = "event"
							}
						case "false_null_kind":
							changed[4], changed[6] = "run_start", nil
						case "changed_generation":
							changed[7] = fact.generation + 1
						}
						if _, err := db.Exec(insert, changed...); err == nil {
							t.Fatal("contradictory canonical live-parent pair committed")
						}
					})
				}
				if _, err := db.Exec(insert, args...); err != nil {
					t.Fatalf("exact pair no longer insertable: %v", err)
				}
			})
		}
	}
}

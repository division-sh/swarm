package main

import (
	"reflect"
	"testing"
)

func TestFamily145PhysicalSQLCorrespondenceRecognizesSelectWhitespaceNotLookalikes(t *testing.T) {
	queries := family145ConnectorSQL(t, `package proof
func fixture() {
 _ = "SELECT\n COUNT(*) FROM events WHERE event_name = 'a b'"
 _ = "SELECT\tCOUNT(*) FROM events WHERE event_name = 'a b'"
 _ = "SELECT COUNT(*) FROM events WHERE event_name = 'a b'"
 _ = "SELECTED COUNT(*) FROM events"
 _ = ""
}`)
	want := []string{
		"SELECTCOUNT(*)FROMeventsWHEREevent_name='a b'",
		"SELECTCOUNT(*)FROMeventsWHEREevent_name='a b'",
		"SELECTCOUNT(*)FROMeventsWHEREevent_name='a b'",
	}
	if !reflect.DeepEqual(queries, want) {
		t.Fatalf("SELECT newline/tab/space inventory, quoted content or lookalike refusal changed: got=%q want=%q", queries, want)
	}
}

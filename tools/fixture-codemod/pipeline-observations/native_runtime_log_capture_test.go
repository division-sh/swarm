package main

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

func runtimeLogCaptureUnitSource(source, function string) string {
	source = strings.ReplaceAll(source, "NewRuntimeLogger(runtimeLogPersistenceStub{capture: capture},", "NewRuntimeLogger(capture,")
	source = strings.ReplaceAll(source, "newTestRuntimeLogger(db, runtimeLogPersistenceStub{capture: capture})", "newTestRuntimeLogger(capture)")
	source = strings.Replace(source, "\tdb, mock, err := sqlmock.New()\n\tif err != nil {\n\t\tt.Fatalf(\"sqlmock: %v\", err)\n\t}\n\tdefer db.Close()\n\n", "", 1)
	expectations := regexp.MustCompile(`\tif err := mock\.ExpectationsWereMet\(\); err != nil \{\n\t\tt\.Fatalf\("(?:ExpectationsWereMet\(\) error = %v|expectations: %v)", err\)\n\t\}\n`)
	source = expectations.ReplaceAllString(source, "")
	source = strings.Replace(source, "\terr = logger.Log(", "\terr := logger.Log(", 1)
	cut := "\tif entries := recorder.SnapshotFlightRecorder(); len(entries) != 0 {"
	switch function {
	case "TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnPayloadValidationFailure":
		source = strings.Replace(source, "\tlogger := newTestRuntimeLogger(db, runtimeLogPersistenceStub{})", "\tcapture := &runtimeLogPersistenceCapture{}\n\tlogger := newTestRuntimeLogger(capture)", 1)
		source = strings.Replace(source, cut, "\tif len(capture.records) != 0 {\n\t\tt.Fatal(\"payload refusal reached persistence\")\n\t}\n"+cut, 1)
	case "TestRuntimeLogger_Log_DoesNotAppendFlightRecorderOnLineageLookupFailure":
		source = strings.Replace(source, "\tmock.ExpectQuery(`SELECT EXISTS`).\n\t\tWithArgs(runID, subjectEventID).\n\t\tWillReturnError(lineageErr)\n", "\tcapture := &runtimeLogPersistenceCapture{lineageErr: lineageErr}\n", 1)
		source = strings.Replace(source, "newTestRuntimeLogger(db, runtimeLogPersistenceStub{})", "newTestRuntimeLogger(capture)", 1)
		source = strings.Replace(source, cut, "\tif len(capture.lineageRequests) != 1 || capture.lineageRequests[0] != ([3]string{runID, \"\", subjectEventID}) || len(capture.records) != 0 {\n\t\tt.Fatalf(\"lineage refusal lost exact arguments or reached persistence: %+v\", capture)\n\t}\n"+cut, 1)
	}
	return source
}

func TestRuntimeLogCaptureOracleRejectsLostPayloadAndFailureEvidence(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Family != "native-runtime-log-capture-unit" {
			continue
		}
		want, err := canonicalFunction(runtimeLogCaptureUnitSource(row.Before, row.Function))
		if err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{"logger.Log", "RuntimeLogEntry", "runtimeLogPayloadArg", "if len(capture.records)", "errors.Is", "SnapshotFlightRecorder", "lineageRequests[0]"} {
			if !strings.Contains(row.After, cut) {
				continue
			}
			mutant := strings.Replace(row.After, cut, "unreviewed", 1)
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("lost original capture contract accepted: %s/%s", row.Function, cut)
			}
		}
	}
}

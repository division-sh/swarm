package main

import (
	"strings"
)

func normalizeOriginReadCleanup(source string) string {
	for _, pair := range [][2]string{
		{"\t\t\tvar barrier *storetest.PostgresRunTableReadBarrier\n\t\t\tvar releaseOnce sync.Once\n\t\t\treleaseReader := func() { releaseOnce.Do(func() { close(reader.release) }) }\n\t\t\tcompleted := make(chan error, 1)\n\t\t\tworkerStarted, workerJoined := false, false\n\t\t\tgeneration := &completeEventDispatchGeneration{}\n\t\t\tt.Cleanup(func() {\n\t\t\t\tclean := true\n\t\t\t\tcancel()\n\t\t\t\tif c != nil {\n\t\t\t\t\twait, stop := context.WithCancel(context.Background())\n\t\t\t\t\tstop()\n\t\t\t\t\t_ = c.Retire(wait)\n\t\t\t\t}\n\t\t\t\tif barrier != nil {\n\t\t\t\t\tif err := barrier.Close(); err != nil {\n\t\t\t\t\t\tclean = false\n\t\t\t\t\t\tt.Error(err)\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t\treleaseReader()\n\t\t\t\tif err := generation.close(); err != nil {\n\t\t\t\t\tclean = false\n\t\t\t\t\tt.Error(err)\n\t\t\t\t}\n\t\t\t\tif workerStarted && !workerJoined {\n\t\t\t\t\tselect {\n\t\t\t\t\tcase <-completed:\n\t\t\t\t\t\tworkerJoined = true\n\t\t\t\t\tcase <-time.After(5 * time.Second):\n\t\t\t\t\t\tclean = false\n\t\t\t\t\t\tt.Error(\"failed origin proof left an unjoined worker\")\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t\tif clean && (owner == nil || owner.ActiveCount() == 0) && (!workerStarted || workerJoined) {\n\t\t\t\t\tt.Log(\"origin proof cleanup joined\")\n\t\t\t\t}\n\t\t\t})\n", ""},
		{"\t\t\t\tgeneration.process = process\n", ""},
		{"\t\t\t\tgeneration.owner = owner\n", ""},
		{"\t\t\t\tgeneration.coordinator = c\n", ""},
		{"\t\t\t\tworkerStarted = true\n", ""},
		{"\t\t\t\tworkerJoined = true\n", ""},
		{"\t\t\treader.enabled.Store(true)\n", "\t\t\treader.enabled.Store(true)\n\t\t\tcompleted := make(chan error, 1)\n"},
		{"\t\t\tvar err error\n\t\t\tbarrier, err = storetest.HoldPostgresRunTableReadBarrier(context.Background(), f.store)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\treleaseReader()\n\t\t\twaitOriginSQLLock(t, f.store)", "\t\t\tlock, err := f.db.BeginTx(context.Background(), nil)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tdefer lock.Rollback()\n\t\t\tif _, err := lock.Exec(`LOCK TABLE runs IN ACCESS EXCLUSIVE MODE`); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tclose(reader.release)\n\t\t\twaitOriginSQLLock(t, f.db)"},
		{"barrier.Close()", "lock.Rollback()"},
		{"\t\t\t\tif err := generation.close(); err != nil {\n\t\t\t\t\tt.Error(err)\n\t\t\t\t}", "\t\t\t\tif _, err := owner.RetireAndWait(context.Background()); err != nil {\n\t\t\t\t\tt.Error(err)\n\t\t\t\t}\n\t\t\t\tprocess.Retire()\n\t\t\t\tif _, err := process.Join(context.Background()); err != nil {\n\t\t\t\t\tt.Error(err)\n\t\t\t\t}"},
	} {
		source = strings.ReplaceAll(source, pair[0], pair[1])
	}
	return source
}

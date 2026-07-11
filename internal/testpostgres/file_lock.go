package testpostgres

import "os"

type fileLock struct {
	file *os.File
}

func (l *fileLock) File() *os.File { return l.file }

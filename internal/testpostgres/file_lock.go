package testpostgres

import "os"

type fileLock struct {
	file *os.File
}

func (l *fileLock) File() *os.File { return l.file }

// Drop closes only this process's descriptor. Unlike Close, it does not issue
// an explicit unlock that could release a lock shared with an inherited child.
func (l *fileLock) Drop() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	return file.Close()
}

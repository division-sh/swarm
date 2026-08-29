//go:build darwin || linux

package startupownership

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"golang.org/x/sys/unix"
)

const sqlitePossessionSuffix = ".possession"

// sqliteFilePossession retains both identities that make selected-store
// possession provable. The database is identity only; exclusion is applied
// exclusively to the engine-disjoint possession coordinate.
type sqliteFilePossession struct {
	mu sync.Mutex

	database     *os.File
	databaseInfo os.FileInfo
	databasePath string

	coordinate     *os.File
	coordinateInfo os.FileInfo
	coordinatePath string

	released bool
}

func acquireSQLiteFilePossession(selectedPath string) (sqlitePossession, error) {
	return acquireSQLitePossession(selectedPath, false)
}

func acquireSQLiteConstructionPossession(selectedPath string) (sqlitePossession, error) {
	return acquireSQLitePossession(selectedPath, true)
}

func acquireSQLitePossession(selectedPath string, createDatabase bool) (sqlitePossession, error) {
	databasePath, err := canonicalSQLiteSelectedPath(selectedPath)
	if err != nil {
		return nil, err
	}
	if err := requireSupportedLocalFilesystem(filepath.Dir(databasePath)); err != nil {
		return nil, err
	}
	if err := prevalidateSQLiteDatabasePath(databasePath, createDatabase); err != nil {
		return nil, err
	}

	coordinatePath := databasePath + sqlitePossessionSuffix
	coordinate, coordinateInfo, err := acquireSQLitePossessionCoordinate(coordinatePath)
	if err != nil {
		return nil, err
	}
	database, databaseInfo, err := openSQLiteDatabaseIdentity(databasePath, createDatabase)
	if err != nil {
		return nil, errors.Join(err, releaseSQLitePossessionCoordinate(coordinate))
	}

	possession := &sqliteFilePossession{
		database:       database,
		databaseInfo:   databaseInfo,
		databasePath:   databasePath,
		coordinate:     coordinate,
		coordinateInfo: coordinateInfo,
		coordinatePath: coordinatePath,
	}
	if err := possession.ProveCurrent(context.Background()); err != nil {
		return nil, errors.Join(err, possession.Release())
	}
	return possession, nil
}

func canonicalSQLiteSelectedPath(selectedPath string) (string, error) {
	selectedPath = strings.TrimSpace(selectedPath)
	if selectedPath == "" {
		return "", errors.New("SQLite selected-store path is required")
	}
	abs, err := filepath.Abs(filepath.Clean(selectedPath))
	if err != nil {
		return "", fmt.Errorf("resolve SQLite selected-store path: %w", err)
	}
	canonicalParent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", fmt.Errorf("resolve SQLite selected-store parent identity: %w", err)
	}
	canonical := filepath.Clean(filepath.Join(canonicalParent, filepath.Base(abs)))
	if filepath.Clean(abs) != canonical && !systemCanonicalPathAlias(abs, canonical) {
		return "", sqlitePriorOwnerAmbiguous(
			"SQLite selected-store aliases are not ownership authority; select its canonical path",
		)
	}
	if info, err := os.Lstat(abs); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", sqlitePriorOwnerAmbiguous(
			"SQLite selected-store aliases are not ownership authority; select its canonical path",
		)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect SQLite selected-store path: %w", err)
	}
	return canonical, nil
}

func prevalidateSQLiteDatabasePath(path string, createDatabase bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if createDatabase {
			return nil
		}
		return fmt.Errorf("open SQLite selected-store identity: %w", err)
	}
	if err != nil {
		return fmt.Errorf("inspect SQLite selected-store identity: %w", err)
	}
	if !isSingleLinkRegularFile(info) {
		return sqlitePriorOwnerAmbiguous(
			"SQLite selected-store hard-link aliases cannot prove one canonical ownership coordinate",
		)
	}
	return nil
}

func acquireSQLitePossessionCoordinate(path string) (*os.File, os.FileInfo, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return nil, nil, sqlitePriorOwnerAmbiguous("SQLite possession coordinate must not be a symbolic link")
		}
		return nil, nil, fmt.Errorf("open SQLite possession coordinate: %w", err)
	}
	coordinate := os.NewFile(uintptr(fd), path)
	if coordinate == nil {
		_ = unix.Close(fd)
		return nil, nil, errors.New("open SQLite possession coordinate file")
	}
	info, err := coordinate.Stat()
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("stat SQLite possession coordinate: %w", err), coordinate.Close())
	}
	if !isSafeSQLitePossessionCoordinate(info) {
		return nil, nil, errors.Join(sqlitePriorOwnerAmbiguous(
			"SQLite possession coordinate must be one owner-only unaliased regular file",
		), coordinate.Close())
	}
	if err := requireSupportedLocalFilesystem(path); err != nil {
		return nil, nil, errors.Join(err, coordinate.Close())
	}
	if err := unix.Flock(int(coordinate.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := coordinate.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, nil, errors.Join(&runtimestartupownership.AcquisitionError{
				Failure: runtimestartupownership.AcquisitionTakeoverRequired,
				Detail:  "selected store is held by another process",
			}, closeErr)
		}
		return nil, nil, errors.Join(fmt.Errorf("acquire SQLite selected-store possession: %w", err), closeErr)
	}
	current, err := os.Lstat(path)
	if err != nil || !isSafeSQLitePossessionCoordinate(current) || !os.SameFile(info, current) {
		return nil, nil, errors.Join(
			sqlitePriorOwnerAmbiguous("SQLite possession coordinate changed during acquisition"),
			releaseSQLitePossessionCoordinate(coordinate),
		)
	}
	return coordinate, info, nil
}

func openSQLiteDatabaseIdentity(path string, create bool) (*os.File, os.FileInfo, error) {
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if create {
		flags |= unix.O_CREAT
	}
	fd, err := unix.Open(path, flags, 0o600)
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return nil, nil, sqlitePriorOwnerAmbiguous(
				"SQLite selected-store aliases are not ownership authority; select its canonical path",
			)
		}
		return nil, nil, fmt.Errorf("open SQLite selected-store identity: %w", err)
	}
	database := os.NewFile(uintptr(fd), path)
	if database == nil {
		_ = unix.Close(fd)
		return nil, nil, errors.New("open SQLite selected-store identity file")
	}
	info, err := database.Stat()
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("stat SQLite selected-store identity: %w", err), database.Close())
	}
	if !isSingleLinkRegularFile(info) {
		return nil, nil, errors.Join(sqlitePriorOwnerAmbiguous(
			"SQLite selected-store hard-link aliases cannot prove one canonical ownership coordinate",
		), database.Close())
	}
	if err := requireSupportedLocalFilesystem(path); err != nil {
		return nil, nil, errors.Join(err, database.Close())
	}
	current, err := os.Lstat(path)
	if err != nil || !isSingleLinkRegularFile(current) || !os.SameFile(info, current) {
		return nil, nil, errors.Join(sqlitePriorOwnerAmbiguous(
			"SQLite selected-store identity changed during acquisition",
		), database.Close())
	}
	return database, info, nil
}

func sqlitePriorOwnerAmbiguous(detail string) error {
	return &runtimestartupownership.AcquisitionError{
		Failure: runtimestartupownership.AcquisitionPriorOwnerAmbiguous,
		Detail:  detail,
	}
}

func isSingleLinkRegularFile(info os.FileInfo) bool {
	if info == nil || !info.Mode().IsRegular() {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}

func isSafeSQLitePossessionCoordinate(info os.FileInfo) bool {
	if !isSingleLinkRegularFile(info) || info.Mode().Perm() != 0o600 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func (p *sqliteFilePossession) ProveCurrent(ctx context.Context) error {
	if p == nil {
		return errors.New("SQLite selected-store possession is missing")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released || p.database == nil || p.coordinate == nil {
		return errors.New("SQLite selected-store possession is released")
	}
	if err := proveSQLitePossessionIdentity(
		p.databasePath, p.databaseInfo, false, "SQLite selected-store file identity changed",
	); err != nil {
		return err
	}
	return proveSQLitePossessionIdentity(
		p.coordinatePath, p.coordinateInfo, true, "SQLite possession coordinate identity changed",
	)
}

func proveSQLitePossessionIdentity(path string, retained os.FileInfo, coordinate bool, detail string) error {
	current, err := os.Lstat(path)
	valid := err == nil && isSingleLinkRegularFile(current)
	if err == nil && coordinate {
		valid = isSafeSQLitePossessionCoordinate(current)
	}
	if err == nil && valid && retained != nil && os.SameFile(retained, current) {
		return nil
	}
	if err == nil {
		err = errors.New(detail)
	} else {
		err = fmt.Errorf("%s: %w", detail, err)
	}
	return &runtimestartupownership.PossessionError{
		Cause: runtimestartupownership.TerminalOwnershipUnprovable,
		Err:   err,
	}
}

func (p *sqliteFilePossession) Release() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		return nil
	}
	p.released = true
	coordinate := p.coordinate
	database := p.database
	p.coordinate = nil
	p.database = nil
	return errors.Join(releaseSQLitePossessionCoordinate(coordinate), closeSQLiteIdentity(database))
}

func releaseSQLitePossessionCoordinate(coordinate *os.File) error {
	if coordinate == nil {
		return nil
	}
	return errors.Join(unix.Flock(int(coordinate.Fd()), unix.LOCK_UN), coordinate.Close())
}

func closeSQLiteIdentity(database *os.File) error {
	if database == nil {
		return nil
	}
	return database.Close()
}

func sameSQLitePossessionResource(left, right sqlitePossession) bool {
	leftFile, leftOK := left.(*sqliteFilePossession)
	rightFile, rightOK := right.(*sqliteFilePossession)
	return leftOK && rightOK && leftFile != nil && rightFile != nil &&
		leftFile.databaseInfo != nil && rightFile.databaseInfo != nil &&
		leftFile.coordinateInfo != nil && rightFile.coordinateInfo != nil &&
		os.SameFile(leftFile.databaseInfo, rightFile.databaseInfo) &&
		os.SameFile(leftFile.coordinateInfo, rightFile.coordinateInfo)
}

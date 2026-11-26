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

	released         bool
	deferredDatabase bool
}

func acquireSQLiteFilePossession(selectedPath string) (sqlitePossession, error) {
	return acquireSQLitePossession(selectedPath, false)
}

func waitSQLiteFilePossession(ctx context.Context, selectedPath string) (sqlitePossession, error) {
	return sqlitePossessionForConstruction(ctx, selectedPath, false, true, false)
}

func acquireSQLiteConstructionPossession(selectedPath string) (sqlitePossession, error) {
	return acquireSQLitePossession(selectedPath, true)
}

func acquireSQLitePossession(selectedPath string, createDatabase bool) (sqlitePossession, error) {
	return sqlitePossessionForConstruction(context.Background(), selectedPath, createDatabase, false, false)
}

func waitSQLiteConstructionPossession(ctx context.Context, selectedPath string, input ...*SQLiteInspectionInput) (sqlitePossession, error) {
	return sqlitePossessionForConstruction(ctx, selectedPath, true, true, false, input...)
}

func waitSQLitePrewritePossession(ctx context.Context, selectedPath string, input ...*SQLiteInspectionInput) (sqlitePossession, error) {
	return sqlitePossessionForConstruction(ctx, selectedPath, true, true, true, input...)
}

func sqlitePossessionForConstruction(ctx context.Context, selectedPath string, createDatabase, wait, deferDatabase bool, input ...*SQLiteInspectionInput) (sqlitePossession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	observed, explicit, err := observeSQLiteConstructionInput(ctx, databasePath, deferDatabase, input)
	if err != nil {
		return nil, err
	}
	coordinate, coordinateInfo, err := lockSQLitePossessionCoordinate(ctx, coordinatePath, observed == nil || observed.coordinate == nil, wait)
	if err != nil {
		return nil, err
	}
	if observed != nil {
		expected := *observed
		if expected.coordinate == nil {
			expected.coordinate = coordinateInfo // Only our first coordinate creation is allowed.
		}
		if !explicit && observed.Absent() {
			// A predecessor may lawfully create its first database before joining.
			// Its observed coordinate, not a speculative absence, is retained.
			current, err := CaptureSQLiteInspectionInput(ctx, databasePath)
			if err != nil {
				return nil, errors.Join(err, releaseSQLitePossessionCoordinate(coordinate))
			}
			expected.file = current.file
		}
		if err := expected.Revalidate(ctx, databasePath); err != nil {
			return nil, errors.Join(err, releaseSQLitePossessionCoordinate(coordinate))
		}
	}
	if deferDatabase {
		current, err := CaptureSQLiteInspectionInput(ctx, databasePath)
		if err != nil {
			return nil, errors.Join(err, releaseSQLitePossessionCoordinate(coordinate))
		}
		possession := &sqliteFilePossession{databasePath: databasePath, databaseInfo: current.file,
			coordinate: coordinate, coordinateInfo: coordinateInfo, coordinatePath: coordinatePath, deferredDatabase: true}
		if err := possession.ProveCurrent(ctx); err != nil {
			return nil, errors.Join(err, possession.Release())
		}
		return possession, nil
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
	if err := possession.ProveCurrent(ctx); err != nil {
		return nil, errors.Join(err, possession.Release())
	}
	return possession, nil
}

func observeSQLiteConstructionInput(ctx context.Context, databasePath string, deferDatabase bool, input []*SQLiteInspectionInput) (*SQLiteInspectionInput, bool, error) {
	if len(input) != 0 && input[0] != nil {
		return input[0], true, nil
	}
	if deferDatabase {
		current, err := CaptureSQLiteInspectionInput(ctx, databasePath)
		if err != nil || !current.Absent() || current.coordinate != nil {
			return current, false, err
		}
	}
	return nil, false, nil
}

func captureSQLiteInspectionCoordinate(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path + sqlitePossessionSuffix)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !isSafeSQLitePossessionCoordinate(info) {
		return nil, sqlitePriorOwnerAmbiguous("SQLite possession coordinate must be one owner-only unaliased regular file")
	}
	return info, nil
}

func (p *sqliteFilePossession) prepareDatabase(ctx context.Context) error {
	if p == nil {
		return errors.New("SQLite construction possession is required")
	}
	if err := p.ProveCurrent(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.deferredDatabase {
		return nil
	}
	database, info, err := openSQLiteDatabaseIdentity(p.databasePath, true, p.databaseInfo == nil)
	if err != nil {
		return err
	}
	if p.databaseInfo != nil && !os.SameFile(p.databaseInfo, info) {
		return errors.Join(errors.New("SQLite selected-store changed before writable construction"), database.Close())
	}
	p.database, p.databaseInfo, p.deferredDatabase = database, info, false
	return nil
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

func openSQLitePossessionCoordinate(path string, create bool) (*os.File, os.FileInfo, error) {
	return lockSQLitePossessionCoordinate(context.Background(), path, create, false)
}

func lockSQLitePossessionCoordinate(ctx context.Context, path string, create, wait bool) (*os.File, os.FileInfo, error) {
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if create {
		flags |= unix.O_CREAT
	}
	fd, err := unix.Open(path, flags, 0o600)
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
	operation := unix.LOCK_EX | unix.LOCK_NB
	if wait {
		operation = unix.LOCK_EX
	}
	// The native wait stays on this caller's stack. Cancellation never closes a
	// descriptor beneath an unjoined syscall; the serving process owns its exit.
	if err := unix.Flock(int(coordinate.Fd()), operation); err != nil {
		identityErr := proveSQLitePossessionIdentity(path, info, true, "SQLite possession coordinate changed during observation")
		closeErr := coordinate.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			occupied := &runtimestartupownership.AcquisitionError{
				Failure: runtimestartupownership.AcquisitionTakeoverRequired,
				Detail:  "selected store is held by another process",
			}
			if identityErr == nil && closeErr == nil {
				return nil, nil, occupied
			}
			return nil, nil, errors.Join(occupied, identityErr, closeErr)
		}
		return nil, nil, errors.Join(fmt.Errorf("acquire SQLite selected-store possession: %w", err), identityErr, closeErr)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, errors.Join(err, releaseSQLitePossessionCoordinate(coordinate))
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

func openSQLiteDatabaseIdentity(path string, create bool, exclusive ...bool) (*os.File, os.FileInfo, error) {
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if create {
		flags |= unix.O_CREAT
	}
	if len(exclusive) != 0 && exclusive[0] {
		flags |= unix.O_EXCL
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

// CaptureSQLiteInspectionIdentity binds non-creating inspection to the same
// database/coordinate identities checked by retained possession.
func CaptureSQLiteInspectionIdentity(path string) (*SQLiteBackendIdentity, error) {
	canonical, err := canonicalSQLiteSelectedPath(path)
	if err != nil {
		return nil, err
	}
	database, info, err := openSQLiteDatabaseIdentity(canonical, false)
	if err != nil {
		return nil, err
	}
	if err := database.Close(); err != nil {
		return nil, err
	}
	coordinateInfo, err := os.Lstat(canonical + sqlitePossessionSuffix)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil && !isSafeSQLitePossessionCoordinate(coordinateInfo) {
		return nil, sqlitePriorOwnerAmbiguous("SQLite possession coordinate must be one owner-only unaliased regular file")
	}
	return &SQLiteBackendIdentity{reference: &sqliteFilePossession{
		databasePath: canonical, databaseInfo: info,
		coordinatePath: canonical + sqlitePossessionSuffix, coordinateInfo: coordinateInfo,
	}}, nil
}

func probeSQLitePossession(ctx context.Context, selectedPath string, identity *SQLiteBackendIdentity) (available bool, resultErr error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := canonicalSQLiteSelectedPath(selectedPath)
	if err != nil {
		return false, err
	}
	if err := requireSupportedLocalFilesystem(filepath.Dir(path)); err != nil {
		return false, err
	}
	if err := prevalidateSQLiteDatabasePath(path, false); err != nil {
		return false, err
	}
	database, databaseInfo, err := openSQLiteDatabaseIdentity(path, false)
	if err != nil {
		return false, err
	}
	defer func() { resultErr = errors.Join(resultErr, database.Close(), ctx.Err()) }()
	if identity == nil {
		return false, errors.New("SQLite possession observation requires opened backend identity")
	}
	reference, ok := identity.reference.(*sqliteFilePossession)
	if !ok || reference.databaseInfo == nil || !os.SameFile(databaseInfo, reference.databaseInfo) {
		return false, sqlitePriorOwnerAmbiguous("SQLite selected-store identity changed after inspection opened")
	}
	if reference.coordinateInfo == nil {
		return false, errors.New("SQLite possession coordinate was missing when inspection opened")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	coordinate, coordinateInfo, err := openSQLitePossessionCoordinate(path+sqlitePossessionSuffix, false)
	if err != nil {
		if occupied, ok := err.(*runtimestartupownership.AcquisitionError); ok && occupied.Failure == runtimestartupownership.AcquisitionTakeoverRequired {
			return false, errors.Join(
				proveSQLitePossessionIdentity(path, databaseInfo, false, "SQLite selected-store file identity changed"),
				proveSQLitePossessionIdentity(path+sqlitePossessionSuffix, reference.coordinateInfo, true, "SQLite possession coordinate changed after inspection opened"),
			)
		}
		return false, errors.Join(err, proveSQLitePossessionIdentity(path, databaseInfo, false, "SQLite selected-store file identity changed"))
	}
	defer func() { resultErr = errors.Join(resultErr, releaseSQLitePossessionCoordinate(coordinate)) }()
	if !os.SameFile(coordinateInfo, reference.coordinateInfo) {
		return false, sqlitePriorOwnerAmbiguous("SQLite possession coordinate changed after inspection opened")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := proveSQLitePossessionIdentity(path, databaseInfo, false, "SQLite selected-store file identity changed"); err != nil {
		return false, err
	}
	if err := proveSQLitePossessionIdentity(path+sqlitePossessionSuffix, coordinateInfo, true, "SQLite possession coordinate identity changed"); err != nil {
		return false, err
	}
	return true, nil
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
	if p.released || p.coordinate == nil || !p.deferredDatabase && p.database == nil {
		return errors.New("SQLite selected-store possession is released")
	}
	if p.deferredDatabase && p.databaseInfo == nil {
		if _, err := os.Lstat(p.databasePath); !errors.Is(err, os.ErrNotExist) {
			return errors.Join(errors.New("SQLite selected-store appeared during boot checks"), err)
		}
	} else {
		if err := proveSQLitePossessionIdentity(p.databasePath, p.databaseInfo, false, "SQLite selected-store file identity changed"); err != nil {
			return err
		}
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

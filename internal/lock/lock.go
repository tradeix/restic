package lock

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/file"
	rid "github.com/restic/restic/internal/id"
	ros "github.com/restic/restic/internal/os"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

// Lock represents a process locking the repository for an operation.
//
// There are two types of locks: exclusive and non-exclusive. There may be many
// different non-exclusive locks, but at most one exclusive lock, which can
// only be acquired while no non-exclusive lock is held.
//
// A lock must be refreshed regularly to not be considered stale, this must be
// triggered by regularly calling Refresh.
type Lock struct {
	Time      time.Time `json:"time"`
	Exclusive bool      `json:"exclusive"`
	Hostname  string    `json:"hostname"`
	Username  string    `json:"username"`
	PID       int       `json:"pid"`
	UID       uint32    `json:"uid,omitempty"`
	GID       uint32    `json:"gid,omitempty"`

	Feedback chan string

	repo   restic.Repository
	lockID *rid.ID
}

// ErrAlreadyLocked is returned when NewLock or NewExclusiveLock are unable to
// acquire the desired lock.
type ErrAlreadyLocked struct {
	otherLock *Lock
}

func (e ErrAlreadyLocked) Error() string {
	s := ""
	if e.otherLock.Exclusive {
		s = "exclusively "
	}
	return fmt.Sprintf("repository is already locked %sby %v", s, e.otherLock)
}

// IsAlreadyLocked returns true iff err is an instance of ErrAlreadyLocked.
func IsAlreadyLocked(err error) bool {
	if _, ok := errors.Cause(err).(ErrAlreadyLocked); ok {
		return true
	}

	return false
}

// NewLock returns a new, non-exclusive lock for the repository. If an
// exclusive lock is already held by another process, ErrAlreadyLocked is
// returned.
func NewLock(ctx context.Context, repo restic.Repository) (*Lock, error) {
	return newLock(ctx, repo, false)
}

// NewExclusiveLock returns a new, exclusive lock for the repository. If
// another lock (normal and exclusive) is already held by another process,
// ErrAlreadyLocked is returned.
func NewExclusiveLock(ctx context.Context, repo restic.Repository) (*Lock, error) {
	return newLock(ctx, repo, true)
}

var waitBeforeLockCheck = 200 * time.Millisecond

// TestSetLockTimeout can be used to reduce the lock wait timeout for tests.
func TestSetLockTimeout(t testing.TB, d time.Duration) {
	t.Logf("setting lock timeout to %v", d)
	waitBeforeLockCheck = d
}

func newLock(ctx context.Context, repo restic.Repository, excl bool) (*Lock, error) {
	lock := &Lock{
		Time:      time.Now(),
		PID:       os.Getpid(),
		Exclusive: excl,
		repo:      repo,
	}

	hn, err := os.Hostname()
	if err == nil {
		lock.Hostname = hn
	}

	if err = lock.fillUserInfo(); err != nil {
		return nil, err
	}

	if err = lock.checkForOtherLocks(ctx); err != nil {
		return nil, err
	}

	lockID, err := lock.createLock(ctx)
	if err != nil {
		return nil, err
	}

	lock.lockID = &lockID

	time.Sleep(waitBeforeLockCheck)

	if err = lock.checkForOtherLocks(ctx); err != nil {
		_ = lock.Unlock()
		return nil, err
	}

	return lock, nil
}

func (l *Lock) fillUserInfo() error {
	usr, err := user.Current()
	if err != nil {
		return nil
	}
	l.Username = usr.Username

	l.UID, l.GID, err = ros.UidGidInt(*usr)
	return err
}

// checkForOtherLocks looks for other locks that currently exist in the repository.
//
// If an exclusive lock is to be created, checkForOtherLocks returns an error
// if there are any other locks, regardless if exclusive or not. If a
// non-exclusive lock is to be created, an error is only returned when an
// exclusive lock is found.
func (l *Lock) checkForOtherLocks(ctx context.Context) error {
	return l.repo.List(ctx, file.LockFile, func(id rid.ID, size int64) error {
		if l.lockID != nil && id.Equal(*l.lockID) {
			return nil
		}

		lock, err := LoadLock(ctx, l.repo, id)
		if err != nil {
			// ignore locks that cannot be loaded
			debug.Log("ignore lock %v: %v", id, err)
			return nil
		}

		if l.Exclusive {
			return ErrAlreadyLocked{otherLock: lock}
		}

		if !l.Exclusive && lock.Exclusive {
			return ErrAlreadyLocked{otherLock: lock}
		}

		return nil
	})
}

// createLock acquires the lock by creating a file in the repository.
func (l *Lock) createLock(ctx context.Context) (rid.ID, error) {
	id, err := l.repo.SaveJSONUnpacked(ctx,  file.LockFile, l)
	if err != nil {
		return rid.ID{}, err
	}

	return id, nil
}

// Unlock removes the lock from the repository.
func (l *Lock) Unlock() error {
	if l == nil || l.lockID == nil {
		return nil
	}

	return l.repo.Backend().Remove(context.TODO(), file.Handle{Type:  file.LockFile, Name: l.lockID.String()})
}

var staleTimeout = 30 * time.Minute

// Stale returns true if the lock is stale. A lock is stale if the timestamp is
// older than 30 minutes or if it was created on the current machine and the
// process isn't alive any more.
func (l *Lock) Stale() bool {
	debug.Log("testing if lock %v for process %d is stale", l, l.PID)
	if time.Since(l.Time) > staleTimeout {
		debug.Log("lock is stale, timestamp is too old: %v\n", l.Time)
		return true
	}

	hn, err := os.Hostname()
	if err != nil {
		debug.Log("unable to find current hostname: %v", err)
		// since we cannot find the current hostname, assume that the lock is
		// not stale.
		return false
	}

	if hn != l.Hostname {
		// lock was created on a different host, assume the lock is not stale.
		return false
	}

	// check if we can reach the process retaining the lock
	exists := l.processExists()
	if !exists {
		debug.Log("could not reach process, %d, lock is probably stale\n", l.PID)
		return true
	}

	debug.Log("lock not stale\n")
	return false
}

// Refresh refreshes the lock by creating a new file in the backend with a new
// timestamp. Afterwards the old lock is removed.
func (l *Lock) Refresh(ctx context.Context) error {
	debug.Log("refreshing lock %v", l.lockID)
	l.Time = time.Now()
	id, err := l.createLock(ctx)
	if err != nil {
		return err
	}

	err = l.repo.Backend().Remove(context.TODO(), file.Handle{Type:  file.LockFile, Name: l.lockID.String()})
	if err != nil {
		return err
	}

	debug.Log("new lock ID %v", id)
	l.lockID = &id

	return nil
}

func (l Lock) String() string {
	text := fmt.Sprintf("PID %d on %s by %s (UID %d, GID %d)\nlock was created at %s (%s ago)\nstorage ID %v",
		l.PID, l.Hostname, l.Username, l.UID, l.GID,
		l.Time.Format("2006-01-02 15:04:05"), time.Since(l.Time),
		l.lockID.Str())

	return text
}

// listen for incoming SIGHUP and ignore
var ignoreSIGHUP sync.Once

func init() {
	ignoreSIGHUP.Do(func() {
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGHUP)
			for s := range c {
				debug.Log("Signal received: %v\n", s)
			}
		}()
	})
}

// LoadLock loads and unserializes a lock from a repository.
func LoadLock(ctx context.Context, repo restic.Repository, id rid.ID) (*Lock, error) {
	lock := &Lock{}
	if err := repo.LoadJSONUnpacked(ctx,  file.LockFile, id, lock); err != nil {
		return nil, err
	}
	lock.lockID = &id

	return lock, nil
}

// RemoveStaleLocks deletes all locks detected as stale from the repository.
func RemoveStaleLocks(ctx context.Context, repo restic.Repository) error {
	return repo.List(ctx,  file.LockFile, func(id rid.ID, size int64) error {
		lock, err := LoadLock(ctx, repo, id)
		if err != nil {
			// ignore locks that cannot be loaded
			debug.Log("ignore lock %v: %v", id, err)
			return nil
		}

		if lock.Stale() {
			return repo.Backend().Remove(context.TODO(), file.Handle{Type:  file.LockFile, Name: id.String()})
		}

		return nil
	})
}

// RemoveAllLocks removes all locks forcefully.
func RemoveAllLocks(ctx context.Context, repo restic.Repository) error {
	return repo.List(ctx,  file.LockFile, func(id rid.ID, size int64) error {
		return repo.Backend().Remove(context.TODO(), file.Handle{Type:  file.LockFile, Name: id.String()})
	})
}

var globalLocks struct {
	locks         []*Lock
	cancelRefresh chan struct{}
	refreshWG     sync.WaitGroup
	sync.Mutex
}

func LockRepo(repo *repository.Repository) (*Lock, error) {
	return lockRepository(repo, false)
}

func LockRepoExclusive(repo *repository.Repository) (*Lock, error) {
	return lockRepository(repo, true)
}

func lockRepository(repo *repository.Repository, exclusive bool) (*Lock, error) {
	lockFn := NewLock
	if exclusive {
		lockFn = NewExclusiveLock
	}

	lock, err := lockFn(context.TODO(), repo)
	if err != nil {
		return nil, errors.WithMessage(err, "unable to create lock in backend")
	}
	debug.Log("create lock %p (exclusive %v)", lock, exclusive)

	globalLocks.Lock()
	if globalLocks.cancelRefresh == nil {
		debug.Log("start goroutine for lock refresh")
		globalLocks.cancelRefresh = make(chan struct{})
		globalLocks.refreshWG = sync.WaitGroup{}
		globalLocks.refreshWG.Add(1)
		go refreshLocks(&globalLocks.refreshWG, globalLocks.cancelRefresh)
	}

	globalLocks.locks = append(globalLocks.locks, lock)
	globalLocks.Unlock()

	return lock, err
}

var refreshInterval = 5 * time.Minute

func refreshLocks(wg *sync.WaitGroup, done <-chan struct{}) {
	debug.Log("start")
	defer func() {
		wg.Done()
		globalLocks.Lock()
		globalLocks.cancelRefresh = nil
		globalLocks.Unlock()
	}()

	ticker := time.NewTicker(refreshInterval)

	for {
		select {
		case <-done:
			debug.Log("terminate")
			return
		case <-ticker.C:
			debug.Log("refreshing locks")
			globalLocks.Lock()
			for _, lock := range globalLocks.locks {
				err := lock.Refresh(context.TODO())
				if err != nil {
					lock.Feedback <- fmt.Sprintf("unable to refresh lock: %v\n", err)
				}
			}
			globalLocks.Unlock()
		}
	}
}

func UnlockRepo(lock *Lock) error {
	globalLocks.Lock()
	defer globalLocks.Unlock()

	for i := 0; i < len(globalLocks.locks); i++ {
		if lock == globalLocks.locks[i] {
			// remove the lock from the repo
			debug.Log("unlocking repository with lock %v", lock)
			if err := lock.Unlock(); err != nil {
				debug.Log("error while unlocking: %v", err)
				return err
			}

			// remove the lock from the list of locks
			globalLocks.locks = append(globalLocks.locks[:i], globalLocks.locks[i+1:]...)
			return nil
		}
	}

	debug.Log("unable to find lock %v in the global list of locks, ignoring", lock)

	return nil
}

func UnlockAll() error {
	globalLocks.Lock()
	defer globalLocks.Unlock()

	debug.Log("unlocking %d locks", len(globalLocks.locks))
	for _, lock := range globalLocks.locks {
		if err := lock.Unlock(); err != nil {
			debug.Log("error while unlocking: %v", err)
			return err
		}
		debug.Log("successfully removed lock")
	}
	globalLocks.locks = globalLocks.locks[:0]

	return nil
}

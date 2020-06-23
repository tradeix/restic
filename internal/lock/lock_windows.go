package lock

import (
	"os"
	"os/user"

	"github.com/restic/restic/internal/debug"
)

// checkProcess will check if the process retaining the lock exists.
// Returns true if the process exists.
func (l Lock) processExists() bool {
	proc, err := os.FindProcess(l.PID)
	if err != nil {
		debug.Log("error searching for process %d: %v\n", l.PID, err)
		return false
	}
	proc.Release()
	return true
}

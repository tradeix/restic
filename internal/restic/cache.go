package restic

import (
	"io"
	"github.com/restic/restic/internal/file"
	"github.com/restic/restic/internal/id"
)

// Cache manages a local cache.
type Cache interface {
	// BaseDir returns the base directory of the cache.
	BaseDir() string

	// Wrap returns a backend with a cache.
	Wrap(Backend) Backend

	// IsNotExist returns true if the error was caused by a non-existing file.
	IsNotExist(err error) bool

	// Load returns a reader that yields the contents of the file with the
	// given id if it is cached. rd must be closed after use. If an error is
	// returned, the ReadCloser is nil. The files are still encrypted
	Load(h  file.Handle, length int, offset int64) (io.ReadCloser, error)

	// SaveIndex saves an index in the cache.
	Save( file.Handle, io.Reader) error

	// SaveWriter returns a writer for the to be cached object h. It must be
	// closed after writing is finished.
	SaveWriter( file.Handle) (io.WriteCloser, error)

	// Remove deletes a single file from the cache. If it isn't cached, this
	// functions must return no error.
	Remove( file.Handle) error

	// Clear removes all files of type t from the cache that are not contained in the set.
	Clear(file.FileType, id.IDSet) error

	// Has returns true if the file is cached.
	Has( file.Handle) bool
}

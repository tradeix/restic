package restic

import (
	"context"

	"github.com/restic/restic/internal/crypto"
	"github.com/restic/restic/internal/file"
	"github.com/restic/restic/internal/id"
)

// Repository stores data in a backend. It provides high-level functions and
// transparently encrypts/decrypts data.
type Repository interface {

	// Backend returns the backend used by the repository
	Backend() Backend

	Key() *crypto.Key

	SetIndex(Index) error

	Index() Index
	SaveFullIndex(context.Context) error
	SaveIndex(context.Context) error
	LoadIndex(context.Context) error

	Config() Config

	LookupBlobSize(id.ID, BlobType) (uint, bool)

	// List calls the function fn for each file of type t in the repository.
	// When an error is returned by fn, processing stops and List() returns the
	// error.
	//
	// The function fn is called in the same Goroutine List() was called from.
	List(ctx context.Context, t file.FileType, fn func(id.ID, int64) error) error
	ListPack(context.Context, id.ID, int64) ([]Blob, int64, error)

	Flush(context.Context) error

	SaveUnpacked(context.Context, file.FileType, []byte) (id.ID, error)
	SaveJSONUnpacked(context.Context, file.FileType, interface{}) (id.ID, error)

	LoadJSONUnpacked(ctx context.Context, t file.FileType, id id.ID, dest interface{}) error
	// LoadAndDecrypt loads and decrypts the file with the given type and ID,
	// using the supplied buffer (which must be empty). If the buffer is nil, a
	// new buffer will be allocated and returned.
	LoadAndDecrypt(ctx context.Context, buf []byte, t file.FileType, id id.ID) (data []byte, err error)

	LoadBlob(context.Context, BlobType, id.ID, []byte) ([]byte, error)
	SaveBlob(context.Context, BlobType, []byte, id.ID, bool) (id.ID, bool, error)

	LoadTree(context.Context, id.ID) (*Tree, error)
	SaveTree(context.Context, *Tree) (id.ID, error)
}

// Lister allows listing files in a backend.
type Lister interface {
	List(context.Context, file.FileType, func(FileInfo) error) error
}

// Index keeps track of the blobs are stored within files.
type Index interface {
	Has(id.ID, BlobType) bool
	Lookup(id.ID, BlobType) ([]PackedBlob, bool)
	Count(BlobType) uint

	// Each returns a channel that yields all blobs known to the index. When
	// the context is cancelled, the background goroutine terminates. This
	// blocks any modification of the index.
	Each(ctx context.Context) <-chan PackedBlob
}

package restic

import (
	"context"
	"io"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/file"
)

type backendReaderAt struct {
	be Backend
	h  file.Handle
}

func (brd backendReaderAt) ReadAt(p []byte, offset int64) (n int, err error) {
	return ReadAt(context.TODO(), brd.be, brd.h, offset, p)
}

// ReaderAt returns an io.ReaderAt for a file in the backend.
func ReaderAt(be Backend, h file.Handle) io.ReaderAt {
	return backendReaderAt{be: be, h: h}
}

// ReadAt reads from the backend handle h at the given position.
func ReadAt(ctx context.Context, be Backend, h file.Handle, offset int64, p []byte) (n int, err error) {
	debug.Log("ReadAt(%v) at %v, len %v", h, offset, len(p))

	err = be.Load(ctx, h, len(p), offset, func(rd io.Reader) (ierr error) {
		n, ierr = io.ReadFull(rd, p)

		return ierr
	})
	if err != nil {
		return 0, err
	}

	debug.Log("ReadAt(%v) ReadFull returned %v bytes", h, n)

	return n, errors.Wrapf(err, "ReadFull(%v)", h)
}

// +build darwin freebsd linux

package fuse

import (
	"github.com/restic/restic/internal/id"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/net/context"
)

// BlobSizeCache caches the size of blobs in the repo.
type BlobSizeCache struct {
	m map[id.ID]uint
}

// NewBlobSizeCache returns a new blob size cache containing all entries from midx.
func NewBlobSizeCache(ctx context.Context, idx restic.Index) *BlobSizeCache {
	m := make(map[id.ID]uint, 1000)
	for pb := range idx.Each(ctx) {
		m[pb.ID] = uint(restic.PlaintextLength(int(pb.Length)))
	}
	return &BlobSizeCache{
		m: m,
	}
}

// Lookup returns the size of the blob id.
func (c *BlobSizeCache) Lookup(id id.ID) (size uint, found bool) {
	if c == nil {
		return 0, false
	}

	size, found = c.m[id]
	return size, found
}

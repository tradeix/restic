package main

import (
	"context"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/file"
	rid "github.com/restic/restic/internal/id"
	"github.com/restic/restic/internal/index"
	"github.com/restic/restic/internal/lock"
	"github.com/restic/restic/internal/restic"

	"github.com/spf13/cobra"
)

var cmdRebuildIndex = &cobra.Command{
	Use:   "rebuild-index [flags]",
	Short: "Build a new index file",
	Long: `
The "rebuild-index" command creates a new index based on the pack files in the
repository.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runRebuildIndex(globalOptions)
	},
}

func init() {
	cmdRoot.AddCommand(cmdRebuildIndex)
}

func runRebuildIndex(gopts GlobalOptions) error {
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	lck, err := lock.LockRepoExclusive(repo)
	defer lock.UnlockRepo(lck)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(gopts.ctx)
	defer cancel()
	return rebuildIndex(ctx, repo, rid.NewIDSet())
}

func rebuildIndex(ctx context.Context, repo restic.Repository, ignorePacks rid.IDSet) error {
	Verbosef("counting files in repo\n")

	var packs uint64
	err := repo.List(ctx, file.DataFile, func(rid.ID, int64) error {
		packs++
		return nil
	})
	if err != nil {
		return err
	}

	bar := newProgressMax(!globalOptions.Quiet, packs-uint64(len(ignorePacks)), "packs")
	idx, invalidFiles, err := index.New(ctx, repo, ignorePacks, bar)
	if err != nil {
		return err
	}

	if globalOptions.verbosity >= 2 {
		for _, id := range invalidFiles {
			Printf("skipped incomplete pack file: %v\n", id)
		}
	}

	Verbosef("finding old index files\n")

	var supersedes rid.IDs
	err = repo.List(ctx, file.IndexFile, func(id rid.ID, size int64) error {
		supersedes = append(supersedes, id)
		return nil
	})
	if err != nil {
		return err
	}

	ids, err := idx.Save(ctx, repo, supersedes)
	if err != nil {
		return errors.Fatalf("unable to save index, last error was: %v", err)
	}

	Verbosef("saved new indexes as %v\n", ids)

	Verbosef("remove %d old index files\n", len(supersedes))

	for _, id := range supersedes {
		if err := repo.Backend().Remove(ctx, file.Handle{
			Type: file.IndexFile,
			Name: id.String(),
		}); err != nil {
			Warnf("error removing old index %v: %v\n", id.Str(), err)
		}
	}

	return nil
}

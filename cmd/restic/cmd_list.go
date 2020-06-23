package main

import (
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/file"
	rid "github.com/restic/restic/internal/id"
	"github.com/restic/restic/internal/lock"
	"github.com/restic/restic/internal/repository"

	"github.com/spf13/cobra"
)

var cmdList = &cobra.Command{
	Use:   "list [blobs|packs|index|snapshots|keys|locks]",
	Short: "List objects in the repository",
	Long: `
The "list" command allows listing objects in the repository based on type.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runList(cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdList)
}

func runList(cmd *cobra.Command, opts GlobalOptions, args []string) error {
	if len(args) != 1 {
		return errors.Fatal("type not specified, usage: " + cmd.Use)
	}

	repo, err := OpenRepository(opts)
	if err != nil {
		return err
	}

	if !opts.NoLock {
		lck, err := lock.LockRepo(repo)
		defer lock.UnlockRepo(lck)
		if err != nil {
			return err
		}
	}

	var t file.FileType
	switch args[0] {
	case "packs":
		t = file.DataFile
	case "index":
		t = file.IndexFile
	case "snapshots":
		t = file.SnapshotFile
	case "keys":
		t = file.KeyFile
	case "locks":
		t = file.LockFile
	case "blobs":
		return repo.List(opts.ctx, file.IndexFile, func(id rid.ID, size int64) error {
			idx, err := repository.LoadIndex(opts.ctx, repo, id)
			if err != nil {
				return err
			}
			for blobs := range idx.Each(opts.ctx) {
				Printf("%v %v\n", blobs.Type, blobs.ID)
			}
			return nil
		})

		return nil
	default:
		return errors.Fatal("invalid type")
	}

	return repo.List(opts.ctx, t, func(id rid.ID, size int64) error {
		Printf("%s\n", id)
		return nil
	})
}

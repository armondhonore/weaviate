//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package copier

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

func TestRewriteRelPathToLocalShard(t *testing.T) {
	c := &Copier{rootDataPath: "/data", logger: logrus.New()}

	tests := []struct {
		name          string
		src, srcShard string
		localShard    string
		want          string
	}{
		{
			name:       "no_override_passes_through",
			src:        "myclass/shard1/segment-1.db",
			srcShard:   "shard1",
			localShard: "shard1",
			want:       "myclass/shard1/segment-1.db",
		},
		{
			name:       "rewrites_shard_segment",
			src:        "myclass/shard1/segment-1.db",
			srcShard:   "shard1",
			localShard: "shard1.recovering",
			want:       "myclass/shard1.recovering/segment-1.db",
		},
		{
			name:       "deep_subpath_preserved",
			src:        "myclass/shard1/lsm/objects/segment-3.db",
			srcShard:   "shard1",
			localShard: "shard1.recovering",
			want:       "myclass/shard1.recovering/lsm/objects/segment-3.db",
		},
		{
			name:       "non_matching_segment_unchanged",
			src:        "myclass/different/file.db",
			srcShard:   "shard1",
			localShard: "shard1.recovering",
			want:       "myclass/different/file.db",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := c.rewriteRelPathToLocalShard(tc.src, tc.srcShard, tc.localShard)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestValidateLocalFolder_MissingBasePath(t *testing.T) {
	root := t.TempDir()
	c := &Copier{rootDataPath: root, logger: logrus.New()}
	require.NoError(t, c.validateLocalFolder("MyClass", "shard1", api.RecoveryFolderName("shard1"), nil))
}

func TestPromoteRecoveryFolder(t *testing.T) {
	root := t.TempDir()
	c := &Copier{rootDataPath: root, logger: logrus.New()}

	collection := "MyClass"
	shard := "shard1"
	livePath := c.shardPath(collection, shard)
	recoveryPath := c.shardPath(collection, api.RecoveryFolderName(shard))

	t.Run("happy_path_renames_recovery_into_place", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(recoveryPath, "marker"), []byte("ok"), 0o644))

		require.NoError(t, c.PromoteRecoveryFolder(collection, shard))

		_, err := os.Stat(recoveryPath)
		require.True(t, errors.Is(err, fs.ErrNotExist), "recovery dir should be gone, got %v", err)
		info, err := os.Stat(livePath)
		require.NoError(t, err)
		require.True(t, info.IsDir())
		_, err = os.Stat(path.Join(livePath, "marker"))
		require.NoError(t, err)

		require.NoError(t, os.RemoveAll(livePath))
	})

	t.Run("erases_stale_recovery_dir_when_live_dir_already_exists", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
		require.NoError(t, os.MkdirAll(livePath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(livePath, "live-marker"), []byte("live"), 0o644))

		require.NoError(t, c.PromoteRecoveryFolder(collection, shard))

		_, statErr := os.Stat(recoveryPath)
		require.True(t, errors.Is(statErr, fs.ErrNotExist), "stale recovery dir should be erased, got %v", statErr)
		_, err := os.Stat(path.Join(livePath, "live-marker"))
		require.NoError(t, err, "live dir contents must not be touched")

		require.NoError(t, os.RemoveAll(livePath))
	})

	t.Run("idempotent_when_live_exists_and_recovery_missing", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(livePath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(livePath, "live-marker"), []byte("live"), 0o644))

		require.NoError(t, c.PromoteRecoveryFolder(collection, shard))

		_, err := os.Stat(path.Join(livePath, "live-marker"))
		require.NoError(t, err, "live dir contents must not be touched")
		_, err = os.Stat(recoveryPath)
		require.True(t, errors.Is(err, fs.ErrNotExist), "no recovery dir should appear, got %v", err)

		require.NoError(t, os.RemoveAll(livePath))
	})

	t.Run("errors_when_both_dirs_missing", func(t *testing.T) {
		err := c.PromoteRecoveryFolder(collection, shard)
		require.Error(t, err)
	})

	t.Run("errors_when_path_is_a_file_not_a_directory", func(t *testing.T) {
		parent := filepath.Dir(livePath)
		require.NoError(t, os.MkdirAll(parent, 0o755))
		require.NoError(t, os.WriteFile(livePath, []byte("oops"), 0o644))
		require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(recoveryPath, "marker"), []byte("recovery"), 0o644))

		err := c.PromoteRecoveryFolder(collection, shard)
		require.Error(t, err, "must error when a non-dir occupies the live path")

		_, statErr := os.Stat(path.Join(recoveryPath, "marker"))
		require.NoError(t, statErr, "recovery dir must be left intact on stat error")

		require.NoError(t, os.Remove(livePath))
		require.NoError(t, os.RemoveAll(recoveryPath))
	})
}

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

package db

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// RecoveringShard wraps a LazyLoadShard for SELF_RECOVERY. Until promoted
// via Load (by the consumer's LoadLocalShard or the orchestrator's
// empty-fallback), inner Load is blocked with ErrShardRecovering so a lazy
// load can't MkdirAll an empty shard before the copy-and-rename completes,
// and GetStatus reports RECOVERING. This is local defense-in-depth; the
// replication FSM read filter handles cluster-wide routing exclusion.
//
// IMPORTANT: while blocked, any inherited data-path method going through
// mustLoad/mustLoadCtx (Store, NotifyReady, put*/delete*/... internals)
// PANICS by design — reaching one is a routing bug. Iterating callers must
// skip recovering shards (loaded accessors, or IsRecovering()). See
// docs/self-recovery.md ("Limitations").
type RecoveringShard struct {
	*LazyLoadShard
}

func NewRecoveringShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	indexCheckpoints *indexcheckpoint.Checkpoints, memMonitor memwatch.AllocChecker,
	shardLoadLimiter *loadlimiter.LoadLimiter, shardReindexer ShardReindexerV3,
	bitmapBufPool roaringset.BitmapBufPool,
) *RecoveringShard {
	inner := NewLazyLoadShard(ctx, promMetrics, shardName, index, class, jobQueueCh,
		indexCheckpoints, memMonitor, shardLoadLimiter, shardReindexer,
		false, bitmapBufPool)
	inner.blockLoad(enterrors.ErrShardRecovering)
	return &RecoveringShard{LazyLoadShard: inner}
}

// Load shadows LazyLoadShard.Load: clears the block, then loads. After it
// returns nil the wrapper behaves like a normal LazyLoadShard.
func (r *RecoveringShard) Load(ctx context.Context) error {
	r.clearLoadBlock()
	return r.LazyLoadShard.Load(ctx)
}

func (r *RecoveringShard) IsRecovering() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return !r.loaded
}

// GetStatus shadows LazyLoadShard.GetStatus (which would report
// LAZY_LOADING) so /nodes can surface RECOVERING distinctly.
func (r *RecoveringShard) GetStatus() storagestate.Status {
	r.mutex.Lock()
	loaded := r.loaded
	inner := r.shard
	r.mutex.Unlock()
	if loaded {
		return inner.GetStatus()
	}
	return storagestate.StatusRecovering
}

func (r *RecoveringShard) GetStatusReason() string {
	r.mutex.Lock()
	loaded := r.loaded
	inner := r.shard
	r.mutex.Unlock()
	if loaded {
		return inner.GetStatusReason()
	}
	return storagestate.StatusRecovering.String()
}

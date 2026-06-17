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

// Package selfrecovery triggers automatic SELF_RECOVERY replication
// ops for shards whose local directories are missing at node startup.
// Wired only into the startup path so newly-added empty replicas via
// scale-out are not misinterpreted as data loss. The actual file copy
// and state machine are handled by the existing replication FSM +
// consumer; this package only probes peers and registers ops.
package selfrecovery

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	replicationtypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// ErrSelfRecoveryCancelled flags an op the FSM reports as CANCELLED, so
// runOne treats it as terminal (don't retry) rather than transient.
var ErrSelfRecoveryCancelled = errors.New("self-recovery op was cancelled")

// ErrSelfRecoveryShardNotInSchema: requested (collection, shard) absent
// from the local schema. REST handler maps it to 404.
var ErrSelfRecoveryShardNotInSchema = errors.New("shard not in local schema")

// ErrSelfRecoveryShardAlreadyLive: live on-disk dir already exists, so
// /restart would re-copy over a healthy shard. REST handler maps it to
// 409. Operators must cancel any in-flight op and remove the dir by hand.
var ErrSelfRecoveryShardAlreadyLive = errors.New("shard already has a live local directory; /restart is only valid while the shard is RECOVERING")

// RaftEntryPoint is the subset of *cluster.Raft used by the orchestrator;
// defined locally so tests can stub it.
type RaftEntryPoint interface {
	RegisterSelfRecovery(ctx context.Context, sourceNode, collection, shard, targetNode string) (strfmt.UUID, error)
	GetReplicationDetailsByReplicationId(ctx context.Context, uuid strfmt.UUID) (api.ReplicationDetailsResponse, error)
	GetReplicationDetailsByCollectionAndShard(ctx context.Context, collection, shard string) ([]api.ReplicationDetailsResponse, error)
	CancelReplication(ctx context.Context, uuid strfmt.UUID) error
}

type SchemaReader interface {
	ShardReplicas(class, shard string) (nodes []string, err error)
}

// PathResolver maps (collection, shard) to the local on-disk dir.
type PathResolver interface {
	ShardPath(collection, shard string) string
}

type ShardRef struct {
	Collection string
	Shard      string
}

// Orchestrator coordinates per-shard SELF_RECOVERY work on a single node.
// It is constructed once at startup and submitted to as the index init
// pass discovers shard directories that should be present but are not.
type Orchestrator struct {
	raft                   RaftEntryPoint
	schema                 SchemaReader
	pathResolver           PathResolver
	clientFactory          copier.FileReplicationServiceClientFactory
	nodeSelector           cluster.NodeSelector
	nodeName               string
	enabled                bool
	concurrency            int
	maintenanceModeEnabled func() bool // nil-safe; treated as off when nil
	onRecoveryComplete     func(ctx context.Context, collection, shard string) error
	logger                 logrus.FieldLogger
	pollInterval           time.Duration
	probeTimeout           time.Duration
	probeBackoffMin        time.Duration
	probeBackoffMax        time.Duration
	restartTimeout         time.Duration  // caps Restart's cancel+settle loop
	vanishedGracePeriod    time.Duration  // extra wait when a polled op vanishes
	emptyFallbackHook      func(ShardRef) // nil unless overridden in tests
	metrics                *Metrics

	// Worker pool. Bounded queue caps goroutine fan-out when
	// N>>concurrency shards need recovery (e.g. wiped node).
	//
	// closeMu serialises Submit's send on workQueue against Close's
	// close of it; closed short-circuits Submit off the hot path.
	// shutdownCtx is cancelled by Close so in-flight probes/polls bail.
	poolOnce            sync.Once
	workQueue           chan submission
	submitQueueCapacity int // defaultSubmitQueueCapacity unless overridden in tests
	closeMu             sync.RWMutex
	closed              atomic.Bool
	workerWg            sync.WaitGroup
	shutdownCtx         context.Context
	shutdownCancel      context.CancelFunc

	// rng shuffles peer order; guarded by rngMu since probeAndDecide
	// runs from multiple workers concurrently.
	rngMu sync.Mutex
	rng   *rand.Rand

	// shardLocks serialises state-mutating ops on the same
	// (collection, shard) across runOne and Restart — defends against
	// concurrent /restart calls and a stale worker racing a new
	// Restart. Keyed "collection/shard" -> *sync.Mutex; never shrinks
	// (fine for self-recovery cardinalities).
	shardLocks sync.Map
}

type submission struct {
	ctx context.Context
	ref ShardRef
	// Captured at submit time (not via a shared flag) so a slow
	// empty-fallback isn't misclassified if RAFT bootstrap completes
	// mid-probe.
	fromBootstrap bool
}

// defaultSubmitQueueCapacity bounds the in-flight backlog; beyond it
// Submit drops (logged + metric) so memory stays bounded on a wiped
// node. Dropped shards fall back to normal init and retry on next restart.
const defaultSubmitQueueCapacity = 1024

type Config struct {
	Raft          RaftEntryPoint
	Schema        SchemaReader
	PathResolver  PathResolver
	ClientFactory copier.FileReplicationServiceClientFactory
	NodeSelector  cluster.NodeSelector
	NodeName      string
	Enabled       bool
	Concurrency   int
	// MaintenanceModeEnabled, when non-nil and true, makes Submit a
	// no-op (no new recoveries during maintenance). Already-running
	// recoveries finish.
	MaintenanceModeEnabled func() bool
	// OnRecoveryComplete promotes the in-memory wrapper after the
	// orchestrator's empty-fallback materialises an empty live dir.
	// (The SELF_RECOVERY-op path doesn't need it; the consumer's
	// LoadLocalShard handles the swap.)
	OnRecoveryComplete func(ctx context.Context, collection, shard string) error
	Logger             logrus.FieldLogger
	// PollInterval is FSM-polling cadence after registering an op. 5s if zero.
	PollInterval time.Duration
	// ProbeTimeout caps a single ListFiles probe RPC. 5s if zero.
	ProbeTimeout time.Duration
}

func New(cfg Config) *Orchestrator {
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	probeTimeout := cfg.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = 5 * time.Second
	}
	logger := cfg.Logger
	if logger == nil {
		logger = logrus.NewEntry(logrus.New())
	}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	return &Orchestrator{
		raft:                   cfg.Raft,
		schema:                 cfg.Schema,
		pathResolver:           cfg.PathResolver,
		clientFactory:          cfg.ClientFactory,
		nodeSelector:           cfg.NodeSelector,
		nodeName:               cfg.NodeName,
		enabled:                cfg.Enabled,
		concurrency:            cfg.Concurrency,
		maintenanceModeEnabled: cfg.MaintenanceModeEnabled,
		onRecoveryComplete:     cfg.OnRecoveryComplete,
		logger:                 logger.WithField("component", "self_recovery"),
		pollInterval:           pollInterval,
		probeTimeout:           probeTimeout,
		probeBackoffMin:        5 * time.Second,
		probeBackoffMax:        5 * time.Minute,
		restartTimeout:         30 * time.Second,
		vanishedGracePeriod:    10 * time.Second,
		submitQueueCapacity:    defaultSubmitQueueCapacity,
		metrics:                GlobalMetrics(),
		rng:                    rand.New(rand.NewSource(cryptoSeed())),
		shutdownCtx:            shutdownCtx,
		shutdownCancel:         shutdownCancel,
	}
}

// cryptoSeed seeds the peer-shuffle math/rand (non-security) so node
// startups pick independent peer orderings without relying on time/pid.
func cryptoSeed() int64 {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		// Unreachable on a sane OS; time+pid still distinct across nodes.
		return time.Now().UnixNano() ^ int64(os.Getpid())
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

// Submit asynchronously starts recovery for the shard. Returns false
// when not queued (flag off, maintenance mode, or queue full). Callers
// that installed a RecoveringShard wrapper MUST fall back to normal
// shard init on false, else the shard stays load-blocked until the next
// restart. fromBootstrap tags the op so a RAFT-bootstrap-window
// empty-fallback is logged/counted less alarmingly (likely a fresh class).
func (o *Orchestrator) Submit(ctx context.Context, ref ShardRef, fromBootstrap bool) bool {
	if !o.enabled {
		return false
	}
	if o.maintenanceModeEnabled != nil && o.maintenanceModeEnabled() {
		o.logger.WithFields(logrus.Fields{
			"event":      "self_recovery.skipped_maintenance_mode",
			"collection": ref.Collection,
			"shard":      ref.Shard,
		}).Info("self-recovery skipped: node is in maintenance mode")
		return false
	}
	o.poolOnce.Do(o.initPool)
	// Read lock covers the send so Close can't close workQueue under us;
	// the closed check off-lock short-circuits the hot path.
	if o.closed.Load() {
		return false
	}
	o.closeMu.RLock()
	defer o.closeMu.RUnlock()
	if o.closed.Load() {
		return false
	}
	select {
	case o.workQueue <- submission{ctx: ctx, ref: ref, fromBootstrap: fromBootstrap}:
		return true
	default:
		if o.metrics != nil {
			o.metrics.SubmitDroppedTotal.Inc()
		}
		o.logger.WithFields(logrus.Fields{
			"event":      "self_recovery.submit_dropped",
			"collection": ref.Collection,
			"shard":      ref.Shard,
			"queue_cap":  o.submitQueueCapacity,
		}).Warn("self-recovery submission dropped: queue full")
		return false
	}
}

// Enabled reports whether the SELF_RECOVERY feature flag is on, so
// callers can gate wrapper installation before Submit (which no-ops off).
func (o *Orchestrator) Enabled() bool {
	return o.enabled
}

// SubmitRecovery is the primitive-typed entry point for callers that
// can't import this package without a cycle. See Submit for the
// false-return contract.
func (o *Orchestrator) SubmitRecovery(ctx context.Context, collection, shard string, fromBootstrap bool) bool {
	return o.Submit(ctx, ShardRef{Collection: collection, Shard: shard}, fromBootstrap)
}

// Restart cancels any in-flight SELF_RECOVERY op for (collection,
// shard) on this node, waits for it to go terminal (so the copier won't
// race the rmrf below), erases "<shard>.recovering/", then resubmits.
// Bounded by restartTimeout; on timeout leaves the recovery dir intact
// and returns ctx.Err().
//
// Rejects with ErrSelfRecoveryShardAlreadyLive when the live dir exists
// (recovery already done) and ErrSelfRecoveryShardNotInSchema when the
// shard isn't in the local schema.
func (o *Orchestrator) Restart(parentCtx context.Context, ref ShardRef) error {
	// Refuse to touch RAFT ops or on-disk paths for unknown shards.
	if o.schema != nil {
		if _, err := o.schema.ShardReplicas(ref.Collection, ref.Shard); err != nil {
			return fmt.Errorf("restart recovery: shard %s/%s: %w",
				ref.Collection, ref.Shard,
				errors.Join(ErrSelfRecoveryShardNotInSchema, err))
		}
	}
	// Serialise the cancel+erase+resubmit below against any worker or
	// concurrent Restart on the same shard.
	unlock := o.lockShard(ref)
	defer unlock()
	if o.pathResolver != nil {
		livePath := o.pathResolver.ShardPath(ref.Collection, ref.Shard)
		if _, err := os.Stat(livePath); err == nil {
			return fmt.Errorf("restart recovery for %s/%s: %w (cancel any in-flight op via POST /replication/replicate/{id}/cancel)",
				ref.Collection, ref.Shard, ErrSelfRecoveryShardAlreadyLive)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("restart recovery: stat live dir %q: %w", livePath, err)
		}
	}

	ctx, cancel := context.WithTimeout(parentCtx, o.restartTimeout)
	defer cancel()

	cancelled, err := o.cancelInflightSelfRecoveryOps(ctx, ref)
	if err != nil {
		return fmt.Errorf("restart recovery: cancel in-flight op(s): %w", err)
	}

	for _, uuid := range cancelled {
		if err := o.waitForOpTerminal(ctx, uuid); err != nil {
			return fmt.Errorf("restart recovery: wait for op %s to settle: %w", uuid, err)
		}
	}

	if o.pathResolver != nil {
		recoveryPath := o.pathResolver.ShardPath(ref.Collection, ref.Shard) + api.RecoveryFolderSuffix
		if err := os.RemoveAll(recoveryPath); err != nil {
			return fmt.Errorf("restart recovery: remove %q: %w", recoveryPath, err)
		}
	}

	o.logger.WithFields(logrus.Fields{
		"event":         "self_recovery.restart",
		"collection":    ref.Collection,
		"shard":         ref.Shard,
		"cancelled_ops": cancelled,
	}).Warn("operator restarted self-recovery from scratch")

	// WithoutCancel so the recovery goroutine outlives the HTTP-bound
	// parentCtx (still inheriting its values). fromBootstrap=false: an
	// operator restart is by definition past the RAFT bootstrap window.
	if !o.Submit(context.WithoutCancel(parentCtx), ref, false) && o.Enabled() {
		// Cancel+erase done but resubmit dropped (queue full); shard
		// stays RECOVERING. Surface so the operator retries.
		return errors.New("restart recovery: re-submission was dropped (in-flight queue full); retry shortly")
	}
	return nil
}

// RestartRecovery is the primitive-typed entry point for callers that
// can't import this package without a cycle.
func (o *Orchestrator) RestartRecovery(ctx context.Context, collection, shard string) error {
	return o.Restart(ctx, ShardRef{Collection: collection, Shard: shard})
}

// cancelInflightSelfRecoveryOps cancels every non-terminal SELF_RECOVERY
// op on (collection, shard) targeting this node. Returns the UUIDs
// cancelled. A "not found" error means no ops at all and is success.
func (o *Orchestrator) cancelInflightSelfRecoveryOps(ctx context.Context, ref ShardRef) ([]strfmt.UUID, error) {
	if o.raft == nil {
		return nil, nil
	}
	ops, err := o.raft.GetReplicationDetailsByCollectionAndShard(ctx, ref.Collection, ref.Shard)
	if err != nil {
		if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var cancelled []strfmt.UUID
	for _, op := range ops {
		if op.TransferType != api.SELF_RECOVERY.String() {
			continue
		}
		if op.TargetNodeId != o.nodeName {
			continue
		}
		state := api.ShardReplicationState(op.Status.State)
		if state == api.READY || state == api.CANCELLED {
			continue
		}
		if err := o.raft.CancelReplication(ctx, op.Uuid); err != nil {
			return cancelled, fmt.Errorf("cancel op %s: %w", op.Uuid, err)
		}
		// Don't tick CompletedTotal{cancelled} here — runOne does it on
		// observing the FSM's CANCELLED state, to avoid double-counting.
		cancelled = append(cancelled, op.Uuid)
	}
	return cancelled, nil
}

// HasInflightReplicationOp reports whether any non-terminal replication
// op (COPY, MOVE, or SELF_RECOVERY) targets (collection, shard) on this
// node. The startup hook calls it before installing a RecoveringShard
// wrapper: otherwise a node restarting mid scale-out COPY would register
// a duplicate SELF_RECOVERY op and clobber the resumed COPY's output on
// rename. Callers should treat a non-nil error as "skip recovery".
func (o *Orchestrator) HasInflightReplicationOp(ctx context.Context, collection, shard string) (bool, error) {
	if o.raft == nil {
		return false, nil
	}
	ops, err := o.raft.GetReplicationDetailsByCollectionAndShard(ctx, collection, shard)
	if err != nil {
		if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
			return false, nil
		}
		return false, err
	}
	for _, op := range ops {
		if op.TargetNodeId != o.nodeName {
			continue
		}
		switch api.ShardReplicationState(op.Status.State) {
		case api.READY, api.CANCELLED:
			// terminal — no longer touching the shard dir
		default:
			return true, nil
		}
	}
	return false, nil
}

// waitForOpTerminal polls the FSM until the op reaches READY or
// CANCELLED. A vanished op (force-deleted upstream) is treated as
// terminal but with an additional grace sleep so a still-running
// consumer goroutine can observe the cancellation. Bounded by the
// caller's ctx.
func (o *Orchestrator) waitForOpTerminal(ctx context.Context, uuid strfmt.UUID) error {
	if o.raft == nil {
		return nil
	}
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		details, err := o.raft.GetReplicationDetailsByReplicationId(ctx, uuid)
		if err != nil {
			if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
				if !sleepCtx(ctx, o.vanishedGracePeriod) {
					return ctx.Err()
				}
				return nil
			}
			// transient (e.g. leader change) — keep polling
		} else {
			switch api.ShardReplicationState(details.Status.State) {
			case api.READY, api.CANCELLED:
				return nil
			case api.REGISTERED, api.HYDRATING, api.FINALIZING, api.DEHYDRATING:
				// non-terminal — keep polling
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// CleanupOrphanRecoveryDirs removes "<shard>.recovering/" dirs whose
// live "<shard>/" sibling exists. Reclaims disk after a
// downgrade-then-upgrade cycle. In-flight recoveries (no sibling) are
// untouched.
func (o *Orchestrator) CleanupOrphanRecoveryDirs(rootDataPath string) ([]string, error) {
	const suffix = api.RecoveryFolderSuffix
	if rootDataPath == "" {
		return nil, errors.New("cleanup orphan recovery dirs: empty root data path")
	}
	collections, err := os.ReadDir(rootDataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read data root %q: %w", rootDataPath, err)
	}
	var removed []string
	for _, c := range collections {
		if !c.IsDir() {
			continue
		}
		collDir := filepath.Join(rootDataPath, c.Name())
		shards, err := os.ReadDir(collDir)
		if err != nil {
			o.logger.WithError(err).WithField("dir", collDir).Warn("cleanup: cannot read collection dir")
			continue
		}
		for _, s := range shards {
			if !s.IsDir() || !strings.HasSuffix(s.Name(), suffix) {
				continue
			}
			recoveryDir := filepath.Join(collDir, s.Name())
			liveDir := filepath.Join(collDir, strings.TrimSuffix(s.Name(), suffix))
			if _, err := os.Stat(liveDir); err != nil {
				continue // no sibling: in-flight recovery to resume
			}
			if err := os.RemoveAll(recoveryDir); err != nil {
				o.logger.WithError(err).WithField("dir", recoveryDir).Warn("cleanup: failed to remove orphan recovery dir")
				continue
			}
			o.logger.WithField("dir", recoveryDir).Info("cleanup: removed orphan recovery dir")
			removed = append(removed, recoveryDir)
		}
	}
	return removed, nil
}

// AcceptEmpty is the operator escape hatch for the catastrophic-wipe
// case (no peer has the data): erases "<shard>.recovering/", creates an
// empty "<shard>/", and promotes the wrapper so the shard is serviceable
// (else it stays load-blocked behind the RecoveringShard wrapper). Does
// NOT cancel in-flight RAFT ops — operator should cancel first.
func (o *Orchestrator) AcceptEmpty(ctx context.Context, ref ShardRef) (string, error) {
	if o.pathResolver == nil {
		return "", errors.New("accept-empty: no PathResolver configured")
	}
	// Refuse unknown (collection, shard) so the endpoint can't create
	// arbitrary paths under the data root.
	if o.schema != nil {
		if _, err := o.schema.ShardReplicas(ref.Collection, ref.Shard); err != nil {
			return "", fmt.Errorf("accept-empty: shard %s/%s: %w",
				ref.Collection, ref.Shard,
				errors.Join(ErrSelfRecoveryShardNotInSchema, err))
		}
	}
	livePath := o.pathResolver.ShardPath(ref.Collection, ref.Shard)
	recoveryPath := livePath + api.RecoveryFolderSuffix

	if _, err := os.Stat(recoveryPath); err == nil {
		if err := os.RemoveAll(recoveryPath); err != nil {
			return "", fmt.Errorf("remove recovery dir %q: %w", recoveryPath, err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		// EACCES, EIO, ELOOP etc. — surface so the operator sees them.
		return "", fmt.Errorf("stat recovery dir %q: %w", recoveryPath, err)
	}
	if err := os.MkdirAll(livePath, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %q: %w", livePath, err)
	}
	if err := diskio.Fsync(filepath.Dir(livePath)); err != nil {
		return "", fmt.Errorf("fsync parent of %q: %w", livePath, err)
	}
	// Promote the wrapper out of RECOVERING; mirrors runOne's
	// empty-fallback path.
	if o.onRecoveryComplete != nil {
		if err := o.onRecoveryComplete(ctx, ref.Collection, ref.Shard); err != nil {
			return "", fmt.Errorf("accept-empty: promote in-memory wrapper for %s/%s: %w",
				ref.Collection, ref.Shard, err)
		}
	}
	if o.metrics != nil {
		o.metrics.AcceptEmptyTotal.Inc()
	}
	o.logger.WithFields(logrus.Fields{
		"event":      "self_recovery.accept_empty",
		"collection": ref.Collection,
		"shard":      ref.Shard,
		"path":       livePath,
	}).Warn("operator accepted empty shard; recovery aborted")
	return livePath, nil
}

// runOne is the per-shard worker: probe peers, act on the decision, and
// back off & retry on transient errors up to maxAttempts. On give-up the
// shard is left in RECOVERING — operators recover via the
// /debug/self-recovery/{restart,accept-empty} endpoints.
func (o *Orchestrator) runOne(ctx context.Context, ref ShardRef, fromBootstrap bool) {
	// Serialise with Restart and other workers on the same shard.
	unlock := o.lockShard(ref)
	defer unlock()
	// Bind the per-op ctx to the orchestrator's shutdown ctx so probes
	// and FSM polls bail when Close fires.
	ctx, cancel := mergedCtx(ctx, o.shutdownCtx)
	defer cancel()

	logger := o.logger.WithFields(logrus.Fields{
		"event":      "self_recovery.started",
		"collection": ref.Collection,
		"shard":      ref.Shard,
	})
	logger.Info("starting self-recovery for shard")

	startedAt := time.Now()
	if o.metrics != nil {
		o.metrics.InProgress.Inc()
		defer o.metrics.InProgress.Dec()
	}

	const maxAttempts = 10
	attempts := 0
	backoff := o.probeBackoffMin

	retryAfterBackoff := func() bool {
		attempts++
		if !sleepCtx(ctx, backoff) {
			return false
		}
		backoff = nextBackoff(backoff, o.probeBackoffMax)
		return true
	}

	for {
		if ctx.Err() != nil {
			return
		}
		if attempts >= maxAttempts {
			logger.WithField("attempts", attempts).Error("self-recovery exhausted retries; giving up. " +
				"Shard stays in RECOVERING — use POST /debug/self-recovery/restart to retry from scratch, " +
				"or POST /debug/self-recovery/accept-empty to accept an empty shard")
			if o.metrics != nil {
				o.metrics.GiveupTotal.Inc()
			}
			o.recordOutcome("failure", "failure", startedAt)
			return
		}

		decision, err := o.probeAndDecide(ctx, ref)
		if err != nil {
			logger.WithError(err).Warn("self-recovery probe failed; will retry")
			if !retryAfterBackoff() {
				return
			}
			continue
		}

		switch decision.action {
		case actionRegisterOp:
			done, retry := o.handleRegisterDecision(ctx, ref, decision, startedAt, logger)
			if done {
				return
			}
			if retry && !retryAfterBackoff() {
				return
			}
		case actionEmptyFallback:
			o.handleEmptyFallback(ctx, ref, decision, startedAt, fromBootstrap, logger)
			return
		case actionRetry:
			logger.WithField("retry_in", backoff.String()).Debug("self-recovery: peers unreachable, will retry")
			if !retryAfterBackoff() {
				return
			}
		}
	}
}

// recordOutcome ticks completed_total and duration_seconds with the
// terminal label (success|failure|empty_fallback|cancelled). Nil-safe.
func (o *Orchestrator) recordOutcome(completedResult, durationResult string, startedAt time.Time) {
	if o.metrics == nil {
		return
	}
	o.metrics.CompletedTotal.WithLabelValues(completedResult).Inc()
	o.metrics.DurationSeconds.WithLabelValues(durationResult).Observe(time.Since(startedAt).Seconds())
}

// handleRegisterDecision registers a SELF_RECOVERY op and polls it to a
// terminal state. Returns done=true when nothing further should be
// attempted (the op reached READY, or was operator-cancelled /
// force-deleted), retry=true when a transient error means the caller
// should back off and probe again.
func (o *Orchestrator) handleRegisterDecision(ctx context.Context, ref ShardRef, decision probeDecision,
	startedAt time.Time, logger logrus.FieldLogger,
) (done, retry bool) {
	if o.metrics != nil {
		o.metrics.StartedTotal.WithLabelValues(decision.sourceNode).Inc()
	}
	err := o.registerAndPoll(ctx, ref, decision.sourceNode)
	if err == nil {
		o.recordOutcome("success", "success", startedAt)
		logger.WithFields(logrus.Fields{
			"event":       "self_recovery.completed",
			"source_node": decision.sourceNode,
			"duration_ms": time.Since(startedAt).Milliseconds(),
		}).Info("self-recovery completed")
		return true, false
	}
	// Force-delete (class/tenant removed) and operator cancel are both
	// terminal: retrying would re-register a fresh op and negate them.
	switch {
	case errors.Is(err, replicationtypes.ErrReplicationOperationNotFound):
		logger.WithError(err).WithField("source_node", decision.sourceNode).
			Info("self-recovery op was force-deleted; abandoning")
		o.recordOutcome("cancelled", "cancelled", startedAt)
		return true, false
	case errors.Is(err, ErrSelfRecoveryCancelled):
		logger.WithError(err).WithField("source_node", decision.sourceNode).
			Info("self-recovery op cancelled; abandoning")
		o.recordOutcome("cancelled", "cancelled", startedAt)
		return true, false
	default:
		logger.WithError(err).WithField("source_node", decision.sourceNode).
			Warn("self-recovery register/poll failed; will retry")
		return false, true
	}
}

// handleEmptyFallback materialises an empty live shard dir, promotes the
// in-memory wrapper, and records the outcome. fromBootstrap selects the
// gentler log/metric treatment for the RAFT-bootstrap-window case (an
// all-peers-empty answer there most likely means a class was added during
// this node's downtime, not data loss).
func (o *Orchestrator) handleEmptyFallback(ctx context.Context, ref ShardRef, decision probeDecision,
	startedAt time.Time, fromBootstrap bool, logger logrus.FieldLogger,
) {
	if err := o.emptyFallback(ref); err != nil {
		logger.WithError(err).Error("self-recovery empty-fallback failed")
		o.recordOutcome("failure", "failure", startedAt)
		return
	}
	// Promote the wrapper. If this fails the shard would be stranded in
	// RECOVERING, so record failure and bail rather than misreport success.
	if o.onRecoveryComplete != nil {
		if err := o.onRecoveryComplete(ctx, ref.Collection, ref.Shard); err != nil {
			logger.WithError(err).Error("self-recovery: promote after empty-fallback failed")
			o.recordOutcome("failure", "failure", startedAt)
			return
		}
	}
	if o.metrics != nil {
		if fromBootstrap {
			o.metrics.NoDataDuringBootstrapTotal.Inc()
		} else {
			o.metrics.NoDataEmptyTotal.Inc()
		}
	}
	o.recordOutcome("empty_fallback", "empty_fallback", startedAt)

	fallbackFields := logrus.Fields{
		"event":        "self_recovery.empty_fallback",
		"probed_peers": decision.probedPeers,
		"duration_ms":  time.Since(startedAt).Milliseconds(),
		"collection":   ref.Collection,
		"shard":        ref.Shard,
		"action_taken": "created_empty_shard",
	}
	if fromBootstrap {
		logger.WithFields(fallbackFields).
			Info("no peer has data for shard during RAFT bootstrap; treating as fresh class")
	} else {
		fallbackFields["recoverable"] = false
		fallbackFields["operator_note"] = "if data is recoverable from backup, restore now"
		logger.WithFields(fallbackFields).
			Warn("no peer has data for shard; created empty shard")
	}
	if o.emptyFallbackHook != nil {
		o.emptyFallbackHook(ref)
	}
}

type recoveryAction int

const (
	actionRegisterOp recoveryAction = iota
	actionEmptyFallback
	actionRetry
)

type probeDecision struct {
	action      recoveryAction
	sourceNode  string
	probedPeers []string
}

func (o *Orchestrator) probeAndDecide(ctx context.Context, ref ShardRef) (probeDecision, error) {
	replicas, err := o.schema.ShardReplicas(ref.Collection, ref.Shard)
	if err != nil {
		return probeDecision{}, fmt.Errorf("read shard replicas: %w", err)
	}

	peers := make([]string, 0, len(replicas))
	for _, n := range replicas {
		if n != o.nodeName {
			peers = append(peers, n)
		}
	}
	if len(peers) == 0 {
		// Only us per schema: nothing to recover from.
		return probeDecision{action: actionEmptyFallback, probedPeers: nil}, nil
	}

	// Shuffle so recovering nodes don't all pick the same data-bearing
	// peer (see cryptoSeed for the RNG rationale).
	o.rngMu.Lock()
	o.rng.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	o.rngMu.Unlock()

	type probeResult struct {
		peer       string
		hasData    bool
		definitive bool
		err        error
	}
	results := make([]probeResult, len(peers))
	var wg sync.WaitGroup
	for i, peer := range peers {
		i, peer := i, peer
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			h, d, e := o.probePeer(ctx, peer, ref)
			results[i] = probeResult{peer: peer, hasData: h, definitive: d, err: e}
		}, o.logger)
	}
	wg.Wait()

	var (
		probedPeers        = make([]string, 0, len(results))
		anyDefinitiveEmpty bool
		anyUnreachable     bool
		firstSource        string
	)
	for _, r := range results {
		probedPeers = append(probedPeers, r.peer)
		if r.err != nil {
			anyUnreachable = true
			if o.metrics != nil {
				o.metrics.UnreachablePeerTotal.WithLabelValues(r.peer).Inc()
			}
			o.logger.WithError(r.err).WithFields(logrus.Fields{
				"event":      "self_recovery.peer_probe",
				"collection": ref.Collection,
				"shard":      ref.Shard,
				"peer":       r.peer,
				"result":     "unreachable",
			}).Debug("peer probe failed")
			continue
		}
		if r.hasData && firstSource == "" {
			firstSource = r.peer
		}
		if r.definitive && !r.hasData {
			anyDefinitiveEmpty = true
		}
	}

	if firstSource != "" {
		return probeDecision{
			action:      actionRegisterOp,
			sourceNode:  firstSource,
			probedPeers: probedPeers,
		}, nil
	}
	if anyUnreachable {
		// Don't silently create empty when probes were inconclusive.
		return probeDecision{action: actionRetry, probedPeers: probedPeers}, nil
	}
	if anyDefinitiveEmpty {
		return probeDecision{action: actionEmptyFallback, probedPeers: probedPeers}, nil
	}
	return probeDecision{action: actionRetry, probedPeers: probedPeers}, nil
}

// probePeer reports whether peer has data for the shard. definitive=true
// means the peer answered (with or without data); err != nil means
// transport/timeout — caller should retry later, not fall through.
func (o *Orchestrator) probePeer(ctx context.Context, peer string, ref ShardRef) (hasData bool, definitive bool, err error) {
	addr := o.nodeSelector.NodeAddress(peer)
	if addr == "" {
		return false, false, fmt.Errorf("no address for peer %q", peer)
	}
	port, err := o.nodeSelector.NodeGRPCPort(peer)
	if err != nil {
		return false, false, fmt.Errorf("get gRPC port for peer %q: %w", peer, err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, o.probeTimeout)
	defer cancel()

	client, err := o.clientFactory(probeCtx, net.JoinHostPort(addr, fmt.Sprintf("%d", port)))
	if err != nil {
		return false, false, fmt.Errorf("connect to peer %q: %w", peer, err)
	}

	resp, err := client.ListFiles(probeCtx, &protocol.ListFilesRequest{
		IndexName: ref.Collection,
		ShardName: ref.Shard,
	})
	if err != nil {
		// gRPC status drives the decision: NotFound = definitive
		// no-data; Unavailable = peer itself recovering / busy →
		// transient. Anything else is treated as transport error.
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.NotFound:
				return false, true, nil
			case codes.Unavailable:
				return false, false, fmt.Errorf("peer %q unavailable: %w", peer, err)
			default:
				// other codes fall through to substring fallback / generic
			}
		}
		// Older peers don't carry typed codes — fall back to
		// substring matching so a rolling upgrade doesn't break.
		if isShardAbsentErr(err) {
			return false, true, nil
		}
		// Shard exists but isn't paused: a positive answer for a
		// read-only probe (see isShardPresentButNotPausedErr).
		if isShardPresentButNotPausedErr(err) {
			return true, true, nil
		}
		return false, false, fmt.Errorf("list files on peer %q: %w", peer, err)
	}
	if resp == nil || len(resp.FileNames) == 0 {
		return false, true, nil
	}
	return true, true, nil
}

// isShardAbsentErr is the rolling-upgrade fallback for peers not yet
// sending typed gRPC codes. Match only shard-specific phrasings — a bare
// "not found" would misclassify e.g. "file X not found" and wrongly
// trigger empty-fallback.
func isShardAbsentErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "incoming list files get shard is nil"):
		return true
	case strings.Contains(msg, "shard is nil"):
		return true
	case strings.Contains(msg, "shard not found"):
		return true
	}
	return false
}

// isShardPresentButNotPausedErr matches the ListFiles error returned
// when the shard exists but file activity has not been paused. The
// probe doesn't pause (the consumer does, when the op runs), so this
// is a positive answer: the source has the shard.
func isShardPresentButNotPausedErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "is not paused for transfer")
}

// registerAndPoll registers a SELF_RECOVERY op and polls the FSM until
// terminal state. nil on READY, error on CANCELLED or poll failure.
// Returns ErrReplicationOperationNotFound if the op vanished from the
// FSM (e.g. operator-driven force-delete after class/tenant deletion).
func (o *Orchestrator) registerAndPoll(ctx context.Context, ref ShardRef, sourceNode string) error {
	uuid, err := o.raft.RegisterSelfRecovery(ctx, sourceNode, ref.Collection, ref.Shard, o.nodeName)
	if err != nil {
		return fmt.Errorf("register self-recovery op: %w", err)
	}

	o.logger.WithFields(logrus.Fields{
		"event":       "self_recovery.op_registered",
		"collection":  ref.Collection,
		"shard":       ref.Shard,
		"source_node": sourceNode,
		"op_uuid":     uuid,
	}).Info("self-recovery op registered; polling for completion")

	// Tolerate a few consecutive not-founds (a leader change can cause
	// them) before concluding the op was force-deleted.
	const notFoundThreshold = 3
	notFoundCount := 0

	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			details, err := o.raft.GetReplicationDetailsByReplicationId(ctx, uuid)
			if err != nil {
				if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
					notFoundCount++
					if notFoundCount >= notFoundThreshold {
						return fmt.Errorf("self-recovery op %s vanished from FSM (force-deleted upstream): %w", uuid, replicationtypes.ErrReplicationOperationNotFound)
					}
				}
				continue // transient (e.g. leader change) — keep polling
			}
			notFoundCount = 0
			switch api.ShardReplicationState(details.Status.State) {
			case api.READY:
				return nil
			case api.CANCELLED:
				return fmt.Errorf("self-recovery op %s: %w", uuid, ErrSelfRecoveryCancelled)
			case api.REGISTERED, api.HYDRATING, api.FINALIZING, api.DEHYDRATING:
				// non-terminal — keep polling
			}
		}
	}
}

// emptyFallback creates an empty live shard dir; reached only when all
// probed peers were reachable and definitively reported no data.
func (o *Orchestrator) emptyFallback(ref ShardRef) error {
	if o.pathResolver == nil {
		return errors.New("empty-fallback: no PathResolver configured")
	}
	dir := o.pathResolver.ShardPath(ref.Collection, ref.Shard)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", dir, err)
	}
	if err := diskio.Fsync(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("fsync parent of %q: %w", dir, err)
	}
	return nil
}

// initPool spawns the worker pool on first Submit; Concurrency workers
// (1 if non-positive) draining a queue of submitQueueCapacity.
func (o *Orchestrator) initPool() {
	n := 1
	if o.concurrency > 0 {
		n = o.concurrency
	}
	capacity := o.submitQueueCapacity
	if capacity <= 0 {
		capacity = defaultSubmitQueueCapacity
	}
	o.workQueue = make(chan submission, capacity)
	for i := 0; i < n; i++ {
		o.workerWg.Add(1)
		enterrors.GoWrapper(func() {
			defer o.workerWg.Done()
			for sub := range o.workQueue {
				o.runOne(sub.ctx, sub.ref, sub.fromBootstrap)
			}
		}, o.logger)
	}
}

// Close stops new submissions, cancels shutdownCtx, and waits for
// workers to drain (bounded by ctx). On ctx deadline returns ctx.Err()
// while workers may still be running (they bail at the next cancellation
// check). Idempotent; safe before the pool is initialised. After Close,
// Submit returns false.
func (o *Orchestrator) Close(ctx context.Context) error {
	o.closeMu.Lock()
	if !o.closed.CompareAndSwap(false, true) {
		o.closeMu.Unlock()
		return nil
	}
	if o.workQueue != nil {
		close(o.workQueue)
	}
	o.closeMu.Unlock()
	o.shutdownCancel()

	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		o.workerWg.Wait()
		close(done)
	}, o.logger)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// mergedCtx returns a context that is cancelled when EITHER parent or
// sibling is cancelled. Used in runOne to bind the per-op ctx to the
// orchestrator's shutdown ctx so workers bail when Close fires.
func mergedCtx(parent, sibling context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(sibling, cancel)
	return ctx, func() { stop(); cancel() }
}

// lockShard acquires the per-shard mutex for ref and returns a release
// closure (use with `defer`). Callers serialised this way: runOne
// (worker) and Restart (operator endpoint).
func (o *Orchestrator) lockShard(ref ShardRef) func() {
	key := ref.Collection + "/" + ref.Shard
	m, _ := o.shardLocks.LoadOrStore(key, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}

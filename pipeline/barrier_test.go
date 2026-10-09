package pipeline

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBarrierMarkIdempotent(t *testing.T) {
	t.Parallel()

	b := newBarrierState(2, uint64(1)<<0|uint64(1)<<1)

	assert.True(t, b.markBranch(0, branchSuccess))
	// repeated mark of the same branch is rejected regardless of status.
	assert.False(t, b.markBranch(0, branchDeadQueue))
	assert.False(t, b.markBranch(0, branchSuccess))

	assert.Equal(t, uint64(1)<<0, b.successMask.Load())
	assert.Equal(t, uint64(0), b.deadQueueMask.Load())
	assert.Equal(t, uint64(1)<<0, b.convergedMask())
}

func TestBarrierCompletionRequired(t *testing.T) {
	t.Parallel()

	b := newBarrierState(2, uint64(1)<<0|uint64(1)<<1)

	assert.False(t, b.isComplete())
	assert.True(t, b.markBranch(0, branchSuccess))
	assert.False(t, b.isComplete(), "barrier must stay open while a required branch is unfinished")
	assert.True(t, b.markBranch(1, branchDeadQueue))
	assert.True(t, b.isComplete(), "required branch converged via dead queue completes the barrier")
}

func TestBarrierCompletionWithOptional(t *testing.T) {
	t.Parallel()

	// branch 0 is required, branch 1 is optional.
	b := newBarrierState(2, uint64(1)<<0)

	assert.True(t, b.markBranch(0, branchSuccess))
	assert.False(t, b.isComplete(), "unfinished optional branch keeps the barrier open")

	assert.True(t, b.markBranch(1, branchAbandoned))
	assert.True(t, b.isComplete())
	assert.Equal(t, uint64(1)<<1, b.abandonedMask.Load())
	assert.Equal(t, uint64(0), b.successMask.Load()&(uint64(1)<<1))
}

func TestBarrierInitFromConverged(t *testing.T) {
	t.Parallel()

	b := newBarrierState(2, uint64(1)<<0|uint64(1)<<1)
	// branch 0 was converged before restart, skip it on replay.
	b.initFromConverged(uint64(1) << 0)

	assert.True(t, b.isConverged(0))
	assert.False(t, b.isConverged(1))
	assert.False(t, b.markBranch(0, branchSuccess), "restored branch must not accept a signal again")
	assert.False(t, b.isComplete())

	assert.True(t, b.markBranch(1, branchSuccess))
	assert.True(t, b.isComplete())
}

func TestBarrierClaimFinalizeOnce(t *testing.T) {
	t.Parallel()

	b := newBarrierState(1, uint64(1))

	assert.True(t, b.claimFinalize())
	assert.False(t, b.claimFinalize())
}

func TestBarrierConcurrentMarks(t *testing.T) {
	t.Parallel()

	const branches = 16
	b := newBarrierState(branches, (uint64(1)<<branches)-1)

	var wg sync.WaitGroup
	for i := range branches {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// hammer the same branch with several signals; exactly one wins.
			var won bool
			for range 4 {
				if b.markBranch(idx, branchSuccess) {
					won = true
				}
				_ = b.markBranch(idx, branchAbandoned)
			}
			assert.True(t, won)
		}(i)
	}
	wg.Wait()

	assert.True(t, b.isComplete())
	assert.Equal(t, (uint64(1)<<branches)-1, b.successMask.Load())
	assert.Equal(t, uint64(0), b.abandonedMask.Load())
}

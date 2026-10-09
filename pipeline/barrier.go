package pipeline

import "go.uber.org/atomic"

// MaxOutputCount limits the number of destinations: branch state is a 64 bit set.
const MaxOutputCount = 64

// branchStatus is the terminal state of one destination branch of an event.
type branchStatus byte

const (
	// branchSuccess: the destination successfully accepted the event.
	branchSuccess branchStatus = iota
	// branchDeadQueue: the event was accepted by the destination's dead queue.
	branchDeadQueue
	// branchAbandoned: the event was dropped for an optional destination per its policy.
	branchAbandoned
)

// barrierState is the per-event shared acknowledgment barrier.
// Every destination branch occupies one bit; a branch becomes terminal only
// once (first signal wins). The pipeline input position is committed only
// when every branch has reached a terminal state.
type barrierState struct {
	branchCount  int
	requiredMask uint64

	// takenMask contains branches which already received any terminal signal.
	takenMask     atomic.Uint64
	successMask   atomic.Uint64
	deadQueueMask atomic.Uint64
	abandonedMask atomic.Uint64

	// finalized guards the single input commit for the event.
	finalized atomic.Bool
}

func newBarrierState(branchCount int, requiredMask uint64) *barrierState {
	return &barrierState{
		branchCount:  branchCount,
		requiredMask: requiredMask,
	}
}

// allMask returns the bit set of every existing branch.
func (b *barrierState) allMask() uint64 {
	return (uint64(1) << b.branchCount) - 1
}

// markBranch records the terminal status of the branch.
// It returns true only for the first terminal signal of the branch;
// repeated or conflicting later signals are rejected.
func (b *barrierState) markBranch(idx int, status branchStatus) bool {
	bit := uint64(1) << idx
	for {
		taken := b.takenMask.Load()
		if taken&bit != 0 {
			return false
		}
		if b.takenMask.CompareAndSwap(taken, taken|bit) {
			break
		}
	}

	switch status {
	case branchSuccess:
		b.successMask.Add(bit)
	case branchDeadQueue:
		b.deadQueueMask.Add(bit)
	case branchAbandoned:
		b.abandonedMask.Add(bit)
	}
	return true
}

// isComplete reports whether every branch has a terminal state.
// Required branches that fail without a converging route never turn terminal,
// so the barrier stays open until they succeed or are dead-queued.
func (b *barrierState) isComplete() bool {
	return b.takenMask.Load()&b.allMask() == b.allMask()
}

// convergedMask returns the snapshot of branches with any terminal state.
func (b *barrierState) convergedMask() uint64 {
	return b.takenMask.Load() & b.allMask()
}

// initFromConverged restores terminal branches loaded from the durable store
// on process restart. The events are replayed by the input, but these branches
// must not be re-delivered.
func (b *barrierState) initFromConverged(mask uint64) {
	mask &= b.allMask()
	b.takenMask.Store(mask)
}

// isConverged reports whether the branch already has a terminal state.
func (b *barrierState) isConverged(idx int) bool {
	return b.takenMask.Load()&(uint64(1)<<idx) != 0
}

// claimFinalize ensures the event is finalized (input committed) only once.
func (b *barrierState) claimFinalize() bool {
	return b.finalized.CompareAndSwap(false, true)
}

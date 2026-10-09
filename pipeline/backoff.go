package pipeline

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type RetriableBatcher struct {
	outFn                RetriableBatcherOutFn
	batcher              *Batcher
	backoffOpts          BackoffOpts
	isDeadQueueAvailable bool
	isFailoverOutput     bool
	onRetryError         func(err error, events []*Event)
}

type RetriableBatcherOutFn func(*WorkerData, *Batch) error

type BackoffOpts struct {
	MinRetention         time.Duration
	Multiplier           float64
	AttemptNum           int
	IsDeadQueueAvailable bool
	// IsFailoverOutput marks the batcher of the failover (deadqueue) output.
	// A failed batch is kept and marked terminal instead of being routed anywhere.
	IsFailoverOutput bool
}

func NewRetriableBatcher(batcherOpts *BatcherOptions, batcherOutFn RetriableBatcherOutFn, opts BackoffOpts, onError func(err error, events []*Event)) *RetriableBatcher {
	batcherBackoff := &RetriableBatcher{
		outFn:                batcherOutFn,
		backoffOpts:          opts,
		onRetryError:         onError,
		isDeadQueueAvailable: opts.IsDeadQueueAvailable,
		isFailoverOutput:     opts.IsFailoverOutput,
	}
	batcherBackoff.setBatcher(batcherOpts)
	return batcherBackoff
}

func (b *RetriableBatcher) setBatcher(batcherOpts *BatcherOptions) {
	batcherOpts.OutFn = b.Out
	b.batcher = NewBatcher(*batcherOpts)
}

func (b *RetriableBatcher) Out(data *WorkerData, batch *Batch) {
	exponentionalBackoff := backoff.ExponentialBackOff{
		InitialInterval:     b.backoffOpts.MinRetention,
		Multiplier:          b.backoffOpts.Multiplier,
		RandomizationFactor: 0.5,
		MaxInterval:         backoff.DefaultMaxInterval,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	exponentionalBackoff.Reset()

	var timer *time.Timer
	numTries := 0
	for {
		err := b.outFn(data, batch)
		if err == nil {
			return
		}
		next := exponentionalBackoff.NextBackOff()
		if next == backoff.Stop || (b.backoffOpts.AttemptNum >= 0 && numTries > b.backoffOpts.AttemptNum) {
			var events []*Event
			if batch != nil {
				events = batch.events
			}
			b.onRetryError(err, events)
			switch {
			case batch != nil && b.isDeadQueueAvailable:
				// main output: events are owned by the failover output, don't commit them here
				batch.reset()
				batch.status = BatchStatusInDeadQueue
			case batch != nil && b.isFailoverOutput:
				// failover output: terminal state, events are committed after the terminal log
				batch.status = BatchStatusFailed
			}
			return
		}
		numTries++
		if timer == nil {
			timer = time.NewTimer(next)
		} else {
			timer.Reset(next)
		}
		<-timer.C
	}
}

func (b *RetriableBatcher) Start(ctx context.Context) {
	b.batcher.Start(ctx)
}

func (b *RetriableBatcher) Stop() {
	b.batcher.Stop()
}

func (b *RetriableBatcher) Add(event *Event) {
	b.batcher.Add(event)
}

func GetBackoff(minRetention time.Duration, multiplier float64, attemptNum uint64) backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = minRetention
	expBackoff.Multiplier = multiplier
	expBackoff.RandomizationFactor = 0.5
	return backoff.WithMaxRetries(expBackoff, attemptNum)
}

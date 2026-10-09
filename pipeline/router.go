package pipeline

import (
	"fmt"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// FailurePolicy defines how a terminal failure of an optional destination converges.
type FailurePolicy string

const (
	// FailurePolicyDiscard drops the event for the failed destination.
	FailurePolicyDiscard FailurePolicy = "discard"
	// FailurePolicyDeadQueue routes the event to the destination's own dead queue.
	FailurePolicyDeadQueue FailurePolicy = "deadqueue"
)

// outputBranch is one named destination of a pipeline.
type outputBranch struct {
	index    int
	name     string
	required bool
	// onFailure is used only for optional branches.
	onFailure FailurePolicy

	info   *OutputPluginInfo
	plugin OutputPlugin

	deadQueueInfo   *OutputPluginInfo
	deadQueuePlugin OutputPlugin

	// branchMetricCtl is non-nil only in multi-output mode; when set, the
	// destination's batcher and plugin metrics are registered in it, isolated
	// from every other destination.
	branchMetricCtl *metric.Ctl

	started atomic.Bool
	healthy atomic.Bool

	healthGauge *metric.Gauge
}

type Router struct {
	branches []*outputBranch

	// finalizeFn is set by the pipeline: commits the input position,
	// marks the stream and returns the event into the pool.
	finalizeFn finalizeFn

	metricCtl *metric.Ctl
	logger    *zap.Logger

	// store is the durable per-destination progress table; nil when disabled.
	store *barrierStore

	eventsMetric *metric.CounterVec // labels: output, result (success|deadqueue|abandoned)
	errorsMetric *metric.CounterVec // labels: output
	healthMetric *metric.GaugeVec   // labels: output
}

func NewRouter() *Router {
	return &Router{}
}

func (r *Router) addBranch(
	info *OutputPluginInfo,
	name string,
	required bool,
	onFailure FailurePolicy,
	branchMetricCtl *metric.Ctl,
) {
	br := &outputBranch{
		index:           len(r.branches),
		name:            name,
		required:        required,
		onFailure:       onFailure,
		info:            info,
		plugin:          info.Plugin.(OutputPlugin),
		branchMetricCtl: branchMetricCtl,
	}
	r.branches = append(r.branches, br)
}

func (br *outputBranch) attachDeadQueue(info *OutputPluginInfo) {
	br.deadQueueInfo = info
	br.deadQueuePlugin = info.Plugin.(OutputPlugin)
}

// SetOutput installs the single main output (legacy configuration).
func (r *Router) SetOutput(info *OutputPluginInfo) {
	r.addBranch(info, info.Type, true, FailurePolicyDiscard, nil)
}

// SetDeadQueueOutput attaches a dead queue to the single output (legacy configuration).
func (r *Router) SetDeadQueueOutput(info *OutputPluginInfo) {
	if len(r.branches) == 0 {
		logger.Panicf("can't set dead queue: output isn't set")
	}
	r.branches[0].attachDeadQueue(info)
}

func (r *Router) hasOutputs() bool {
	return len(r.branches) > 0
}

func (r *Router) mainOutputPlugin() OutputPlugin {
	return r.branches[0].plugin
}

func (r *Router) requiredMask() uint64 {
	var mask uint64
	for _, br := range r.branches {
		if br.required {
			mask |= uint64(1) << br.index
		}
	}
	return mask
}

// registerMetrics creates the destination-aggregate router metrics.
// It is idempotent because the metric ctl caches collectors by name.
func (r *Router) registerMetrics() {
	r.eventsMetric = r.metricCtl.RegisterCounterVec(
		"output_destination_events_total",
		"Count of events converged by pipeline output destinations",
		"output", "result",
	)
	r.errorsMetric = r.metricCtl.RegisterCounterVec(
		"output_destination_errors_total",
		"Count of terminal errors on pipeline output destinations",
		"output",
	)
	r.healthMetric = r.metricCtl.RegisterGaugeVec(
		"output_destination_health",
		"Health of pipeline output destinations: 1 is healthy, 0 has an error",
		"output",
	)
}

// Start starts every destination branch with destination-scoped params.
func (r *Router) Start(template *OutputPluginParams) {
	for _, br := range r.branches {
		params := r.branchParams(template, br)

		br.healthGauge = r.healthMetric.WithLabelValues(br.name)
		br.setHealth(false)
		br.plugin.Start(br.info.Config, params)
		br.started.Store(true)
		br.setHealth(true)

		if br.deadQueuePlugin != nil {
			dqParams := *params
			dqParams.Controller = &deadQueueController{router: r, branch: br}
			dqParams.Router = &deadQueueStubRouter{name: br.name}
			dqParams.OutputName = br.name + "_deadqueue"
			if dqParams.Logger != nil {
				dqParams.Logger = dqParams.Logger.Named("deadqueue")
			}
			br.deadQueuePlugin.Start(br.deadQueueInfo.Config, &dqParams)
		}
	}
}

func (r *Router) branchParams(template *OutputPluginParams, br *outputBranch) *OutputPluginParams {
	params := *template

	if br.branchMetricCtl != nil {
		params.MetricCtl = br.branchMetricCtl
	}
	params.Controller = &branchController{router: r, branch: br}
	params.Router = &branchRouter{router: r, branch: br}
	params.OutputName = br.name
	if template.Logger != nil {
		params.Logger = template.Logger.Named(br.name)
	}
	return &params
}

// Stop stops every destination and its dead queue independently.
func (r *Router) Stop() {
	for _, br := range r.branches {
		br.plugin.Stop()
		if br.deadQueuePlugin != nil {
			br.deadQueuePlugin.Stop()
		}
		br.started.Store(false)
		br.setHealth(false)
	}
}

// Out fans the event out to every destination branch.
func (r *Router) Out(event *Event) {
	if event.IsChildKind() {
		// child events are delivered to all destinations without their own
		// barrier; their commits don't drive the input position.
		r.fanOut(event, nil)
		return
	}

	b := newBarrierState(len(r.branches), r.requiredMask())
	event.barrier = b
	if r.store != nil {
		b.initFromConverged(r.store.converged(uint64(event.SourceID), event.Offset))
	}
	r.fanOut(event, b)
}

func (r *Router) fanOut(event *Event, b *barrierState) {
	for _, br := range r.branches {
		if b != nil && b.isConverged(br.index) {
			continue
		}
		br.plugin.Out(event)
	}
}

// ack handles a successful event signal from one destination.
func (r *Router) ack(event *Event, idx int) {
	b := event.barrier
	if b == nil {
		// child events and other untracked events don't coordinate anything.
		return
	}
	if !b.markBranch(idx, branchSuccess) {
		r.logger.Debug("ignored repeated destination success",
			zap.String("output", r.branches[idx].name))
		return
	}

	br := r.branches[idx]
	r.eventsMetric.WithLabelValues(br.name, "success").Inc()
	r.recordProgress(event, idx)
	br.setHealth(true)
	r.finalizeIfComplete(event)
}

// failBranch handles a terminal failure signal from one destination.
func (r *Router) failBranch(event *Event, idx int) {
	br := r.branches[idx]
	r.errorsMetric.WithLabelValues(br.name).Inc()
	br.setHealth(false)

	b := event.barrier
	if b == nil {
		// an untracked event (e.g. child): route to the destination dead queue as before.
		if br.deadQueuePlugin != nil {
			br.deadQueuePlugin.Out(event)
		} else {
			r.logger.Error("event failed at destination and no dead queue is configured",
				zap.String("output", br.name))
		}
		return
	}

	switch {
	case br.deadQueuePlugin != nil:
		// the branch converges when its own dead queue commits the event.
		br.deadQueuePlugin.Out(event)
	case !br.required && br.onFailure == FailurePolicyDiscard:
		if b.markBranch(idx, branchAbandoned) {
			r.eventsMetric.WithLabelValues(br.name, "abandoned").Inc()
			r.recordProgress(event, idx)
			r.finalizeIfComplete(event)
		}
	default:
		// A required destination gave up without a dead queue (or an optional
		// destination is misconfigured). The barrier stays open, so the input
		// position is not committed; the plugin's own fatal policy may exit
		// the process, otherwise the event is retried after restart.
		r.logger.Error("event failed at destination without a converging route, position is not committed",
			zap.String("output", br.name),
			zap.Bool("required", br.required))
	}
}

// deadQueueAck handles a successful event signal from a destination dead queue.
func (r *Router) deadQueueAck(event *Event, idx int) {
	b := event.barrier
	if b == nil {
		return
	}
	if !b.markBranch(idx, branchDeadQueue) {
		return
	}

	br := r.branches[idx]
	r.eventsMetric.WithLabelValues(br.name, "deadqueue").Inc()
	r.recordProgress(event, idx)
	br.setHealth(false)
	r.finalizeIfComplete(event)
}

func (r *Router) finalizeIfComplete(event *Event) {
	b := event.barrier
	if !b.isComplete() || !b.claimFinalize() {
		return
	}
	if r.store != nil {
		r.store.deleteEntry(uint64(event.SourceID), event.Offset)
	}
	r.finalizeFn(event, true, true)
}

func (r *Router) recordProgress(event *Event, idx int) {
	if r.store == nil {
		return
	}
	r.store.record(uint64(event.SourceID), event.Offset, idx)
}

func (br *outputBranch) setHealth(ok bool) {
	br.healthy.Swap(ok)
	if br.healthGauge != nil {
		if ok {
			br.healthGauge.Set(1)
		} else {
			br.healthGauge.Set(0)
		}
	}
}

// Fail implements OutputRouter for contexts that construct a router without
// per-branch scoping (e.g. plugin unit tests); it operates on the first
// branch, matching the legacy router behavior.
func (r *Router) Fail(event *Event) {
	if len(r.branches) == 0 {
		return
	}
	r.failBranch(event, 0)
}

func (r *Router) IsDeadQueueAvailable() bool {
	return len(r.branches) > 0 && r.branches[0].deadQueuePlugin != nil
}

func (r *Router) dump() string {
	o := logger.Header("outputs")
	for _, br := range r.branches {
		health := "ACTIVE"
		if !br.healthy.Load() {
			health = "ERROR"
		}
		if !br.started.Load() {
			health = "STOPPED"
		}
		policy := ""
		if !br.required {
			policy = fmt.Sprintf(", on_failure=%s", br.onFailure)
		}
		o += fmt.Sprintf("#%d %s, type=%s, required=%t%s, health=%s, deadqueue=%t\n",
			br.index, br.name, br.info.Type, br.required, policy, health, br.deadQueuePlugin != nil)
	}
	return o
}

// branchController is the destination-scoped OutputPluginController.
type branchController struct {
	router *Router
	branch *outputBranch
}

func (c *branchController) Commit(event *Event) {
	c.router.ack(event, c.branch.index)
}

func (c *branchController) Error(err string) {
	c.router.logger.Error("output plugin error",
		zap.String("output", c.branch.name),
		zap.String("error", err))
}

// deadQueueController is the OutputPluginController given to a destination dead queue.
type deadQueueController struct {
	router *Router
	branch *outputBranch
}

func (c *deadQueueController) Commit(event *Event) {
	c.router.deadQueueAck(event, c.branch.index)
}

func (c *deadQueueController) Error(err string) {
	c.router.logger.Error("dead queue plugin error",
		zap.String("output", c.branch.name),
		zap.String("error", err))
}

// branchRouter is the destination-scoped OutputRouter given to an output plugin.
type branchRouter struct {
	router *Router
	branch *outputBranch
}

func (r *branchRouter) Fail(event *Event) {
	r.router.failBranch(event, r.branch.index)
}

func (r *branchRouter) IsDeadQueueAvailable() bool {
	return r.branch.deadQueuePlugin != nil
}

// deadQueueStubRouter is the OutputRouter of a dead queue plugin:
// recursive dead queues aren't supported.
type deadQueueStubRouter struct {
	name string
}

func (r *deadQueueStubRouter) Fail(*Event) {
	logger.Errorf("dead queue for output %q failed to persist event", r.name)
}

func (r *deadQueueStubRouter) IsDeadQueueAvailable() bool {
	return false
}

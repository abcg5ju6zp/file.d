package pipeline

import (
	"github.com/ozontech/file.d/metric"
	"go.uber.org/zap"
)

type Router struct {
	output     OutputPlugin
	outputInfo *OutputPluginInfo

	deadQueue     OutputPlugin
	deadQueueInfo *OutputPluginInfo

	logger *zap.Logger

	eventsFailoverTotal    *metric.Counter
	eventsTerminalFailures *metric.Counter
}

func NewRouter() *Router {
	return &Router{}
}

func (r *Router) SetOutput(info *OutputPluginInfo) {
	r.outputInfo = info
	r.output = info.Plugin.(OutputPlugin)
}

func (r *Router) SetDeadQueueOutput(info *OutputPluginInfo) {
	r.deadQueueInfo = info
	r.deadQueue = info.Plugin.(OutputPlugin)
}

func (r *Router) Ack(event *Event) {
	// TODO: send commit to input after receiving all acks from outputs
}

// Fail handles an event that wasn't delivered by an output.
// The behavior depends on the event route:
//   - event from the main output is transferred to the failover output exactly once;
//   - event from the failover output reaches the terminal state, it isn't routed anywhere.
func (r *Router) Fail(event *Event, cause error) {
	if event.Route == OutputRouteFailover {
		r.terminalFail(event, cause)
		return
	}

	if !r.IsDeadQueueAvailable() {
		return
	}

	// record the original source and the fact that the main output has failed:
	// the event keeps its SourceID/SourceName/Offset and gets the failover mark.
	event.Route = OutputRouteFailover
	event.failoverReason = cause

	r.eventsFailoverTotal.Inc()
	r.logger.Info("main output failed, transferring event to the failover output",
		zap.String("route", event.Route.String()),
		zap.Uint64("source_id", uint64(event.SourceID)),
		zap.String("source_name", event.SourceName),
		zap.Int64("offset", event.Offset),
		zap.String("main_output", r.outputInfo.Type),
		zap.String("failover_output", r.deadQueueInfo.Type),
		zap.Error(cause),
	)

	r.deadQueue.Out(event)
}

// terminalFail is the actionable terminal state for an event when the failover
// output has failed too. The event is neither re-routed (no recursion) nor
// silently dropped: its origin, both errors and the payload are logged and the
// terminal failure metric is incremented. The failover batcher commits the
// event afterwards, so the offset movement is observable as well.
func (r *Router) terminalFail(event *Event, cause error) {
	r.eventsTerminalFailures.Inc()
	r.logger.Error("failover output failed too, event reached the terminal state",
		zap.String("route", event.Route.String()),
		zap.Uint64("source_id", uint64(event.SourceID)),
		zap.String("source_name", event.SourceName),
		zap.Int64("offset", event.Offset),
		zap.String("main_output", r.outputInfo.Type),
		zap.NamedError("main_output_error", event.failoverReason),
		zap.String("failover_output", r.deadQueueInfo.Type),
		zap.NamedError("failover_output_error", cause),
		zap.String("event", event.Root.EncodeToString()),
	)
}

func (r *Router) Out(event *Event) {
	r.output.Out(event)
}

func (r *Router) Stop() {
	r.output.Stop()
	if r.IsDeadQueueAvailable() {
		r.deadQueue.Stop()
	}
}

func (r *Router) IsDeadQueueAvailable() bool {
	return r.deadQueue != nil
}

func (r *Router) Start(params *OutputPluginParams) {
	r.logger = params.Logger.Desugar().Named("router")
	r.eventsFailoverTotal = params.MetricCtl.RegisterCounter(
		"router_events_failover_total",
		"Count of events transferred from the main output to the failover output",
	)
	r.eventsTerminalFailures = params.MetricCtl.RegisterCounter(
		"router_events_terminal_failed_total",
		"Count of events failed in both main and failover outputs (terminal state)",
	)

	mainParams := *params
	mainParams.Router = r
	mainParams.Role = OutputRoleMain
	r.output.Start(r.outputInfo.Config, &mainParams)

	if r.IsDeadQueueAvailable() {
		failoverParams := *params
		failoverParams.Router = r
		failoverParams.Role = OutputRoleFailover
		// distinguish the failover output results in logs
		failoverParams.Logger = params.Logger.Named("deadqueue")
		r.deadQueue.Start(r.deadQueueInfo.Config, &failoverParams)
	}
}

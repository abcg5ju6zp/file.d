package pipeline_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// countingInput counts input position commits.
type countingInput struct {
	commits atomic.Int64
}

func (i *countingInput) Start(_ pipeline.AnyConfig, _ *pipeline.InputPluginParams) {}
func (i *countingInput) Stop()                                                     {}
func (i *countingInput) Commit(_ *pipeline.Event)                                  { i.commits.Add(1) }
func (i *countingInput) PassEvent(_ *pipeline.Event) bool                          { return true }

// controllableOutput is a test destination with externally driven terminal signals.
type controllableOutput struct {
	received atomic.Int32

	mu   sync.Mutex
	last *pipeline.Event

	onOut      func(event *pipeline.Event)
	controller pipeline.OutputPluginController
	router     pipeline.OutputRouter
}

func (o *controllableOutput) Start(_ pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	o.controller = params.Controller
	o.router = params.Router
}

func (o *controllableOutput) Stop() {}

func (o *controllableOutput) Out(event *pipeline.Event) {
	o.mu.Lock()
	o.last = event
	o.mu.Unlock()

	o.received.Add(1)
	if o.onOut != nil {
		o.onOut(event)
	}
}

func (o *controllableOutput) succeed(event *pipeline.Event) {
	o.controller.Commit(event)
}

func (o *controllableOutput) fail(event *pipeline.Event) {
	o.router.Fail(event)
}

func (o *controllableOutput) pendingEvent() *pipeline.Event {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.last
}

func newBarrierTestPipeline(t *testing.T) (*pipeline.Pipeline, *countingInput, *prometheus.Registry) {
	t.Helper()

	const pipelineName = "router_test"

	registry := prometheus.NewRegistry()
	settings := &pipeline.Settings{
		Capacity:            256,
		MaintenanceInterval: time.Hour,
		EventTimeout:        30 * time.Second,
		Antispam: pipeline.AntispamSettings{
			Threshold: pipeline.DefaultAntispamThreshold,
		},
		AvgEventSize:      2048,
		MetaCacheSize:     32,
		StreamField:       "stream",
		Decoder:           "json",
		OutputOffsetsFile: pipeline.OffOutputOffsets,
		Metric: &pipeline.MetricSettings{
			HoldDuration: pipeline.DefaultMetricHoldDuration,
		},
	}

	p := pipeline.New(pipelineName, settings, registry, zap.NewNop())
	p.DisableParallelism()

	input := &countingInput{}
	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:   "counting",
			Config: struct{}{},
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: input,
		},
	})

	return p, input, registry
}

func addControllableOutput(
	t *testing.T,
	p *pipeline.Pipeline,
	name string,
	required bool,
	onFailure pipeline.FailurePolicy,
) *controllableOutput {
	if t != nil {
		t.Helper()
	}

	out := &controllableOutput{}
	p.AddOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:   name,
			Config: struct{}{},
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: out,
		},
	}, name, required, onFailure, nil)

	return out
}

func attachControllableDeadQueue(t *testing.T, p *pipeline.Pipeline, owner, name string) *controllableOutput {
	t.Helper()

	out := &controllableOutput{}
	err := p.AttachDeadQueue(owner, &pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:   name,
			Config: struct{}{},
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: out,
		},
	})
	require.NoError(t, err)
	return out
}

func feedTestEvent(p *pipeline.Pipeline, sourceID pipeline.SourceID, offset int64) {
	p.In(sourceID, "test_source", test.NewOffset(offset), []byte("{}"), false, nil)
}

// destinationResult reads the router aggregate result counter for one destination.
func destinationResult(t *testing.T, registry *prometheus.Registry, output, result string) float64 {
	t.Helper()

	mfs, err := registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "file_d_pipeline_router_test_output_destination_events_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["output"] == output && labels["result"] == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func healthGauge(t *testing.T, registry *prometheus.Registry, output string) float64 {
	t.Helper()

	mfs, err := registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "file_d_pipeline_router_test_output_destination_health" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "output" && l.GetValue() == output {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return -1
}

func TestMultiOutputFanOut(t *testing.T) {
	t.Parallel()

	p, input, _ := newBarrierTestPipeline(t)
	out1 := addControllableOutput(t, p, "search", true, pipeline.FailurePolicyDiscard)
	out2 := addControllableOutput(t, p, "archive", true, pipeline.FailurePolicyDiscard)
	out3 := addControllableOutput(t, p, "audit", true, pipeline.FailurePolicyDiscard)

	for _, out := range []*controllableOutput{out1, out2, out3} {
		out.onOut = out.succeed
	}

	p.Start()
	defer p.Stop()

	feedTestEvent(p, 1, 1)

	assert.Eventually(t, func() bool {
		return input.commits.Load() == 1
	}, time.Second, 10*time.Millisecond, "input position must be committed once after all branches succeed")

	assert.Equal(t, int32(1), out1.received.Load())
	assert.Equal(t, int32(1), out2.received.Load())
	assert.Equal(t, int32(1), out3.received.Load())
}

func TestBarrierCommitsOnlyAfterAllRequired(t *testing.T) {
	t.Parallel()

	p, input, _ := newBarrierTestPipeline(t)
	fast := addControllableOutput(t, p, "search", true, pipeline.FailurePolicyDiscard)
	slow := addControllableOutput(t, p, "archive", true, pipeline.FailurePolicyDiscard)

	fast.onOut = fast.succeed
	// slow branch holds the event without a terminal signal.

	p.Start()
	defer p.Stop()

	feedTestEvent(p, 2, 10)

	require.Eventually(t, func() bool {
		return slow.received.Load() == 1
	}, time.Second, 10*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(0), input.commits.Load(), "position must not be committed while a required branch is unfinished")

	slow.succeed(slow.pendingEvent())

	assert.Eventually(t, func() bool {
		return input.commits.Load() == 1
	}, time.Second, 10*time.Millisecond)
}

func TestFailedBranchRetryDoesNotResendSucceeded(t *testing.T) {
	t.Parallel()

	p, input, _ := newBarrierTestPipeline(t)
	ok := addControllableOutput(t, p, "search", true, pipeline.FailurePolicyDiscard)
	bad := addControllableOutput(t, p, "archive", true, pipeline.FailurePolicyDiscard)

	ok.onOut = ok.succeed
	bad.onOut = bad.fail // required branch fails terminally without a dead queue, stays unfinished

	p.Start()
	defer p.Stop()

	feedTestEvent(p, 3, 20)

	require.Eventually(t, func() bool {
		return bad.received.Load() == 1 && ok.received.Load() == 1
	}, time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(0), input.commits.Load())

	// the branch internally retries and succeeds with the same event pointer.
	bad.succeed(bad.pendingEvent())

	assert.Eventually(t, func() bool {
		return input.commits.Load() == 1
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, int32(1), ok.received.Load(), "already succeeded branch must not be re-delivered")
}

func TestOptionalBranchDiscardConverges(t *testing.T) {
	t.Parallel()

	p, input, registry := newBarrierTestPipeline(t)
	req := addControllableOutput(t, p, "search", true, pipeline.FailurePolicyDiscard)
	opt := addControllableOutput(t, p, "archive", false, pipeline.FailurePolicyDiscard)

	req.onOut = req.succeed
	opt.onOut = opt.fail

	p.Start()
	defer p.Stop()

	feedTestEvent(p, 4, 30)

	assert.Eventually(t, func() bool {
		return input.commits.Load() == 1
	}, time.Second, 10*time.Millisecond,
		"event commits once the required branch succeeds and the optional one is abandoned")

	assert.Equal(t, float64(0), destinationResult(t, registry, "archive", "success"),
		"an abandoned branch must not be counted as success")
	assert.Equal(t, float64(1), destinationResult(t, registry, "archive", "abandoned"))
	assert.Equal(t, float64(1), destinationResult(t, registry, "search", "success"))
}

func TestOptionalBranchDeadQueueConverges(t *testing.T) {
	t.Parallel()

	p, input, registry := newBarrierTestPipeline(t)
	req := addControllableOutput(t, p, "search", true, pipeline.FailurePolicyDiscard)
	opt := addControllableOutput(t, p, "archive", false, pipeline.FailurePolicyDeadQueue)
	optDLQ := attachControllableDeadQueue(t, p, "archive", "archive_dlq")

	req.onOut = req.succeed
	opt.onOut = opt.fail
	optDLQ.onOut = optDLQ.succeed

	p.Start()
	defer p.Stop()

	feedTestEvent(p, 5, 40)

	assert.Eventually(t, func() bool {
		return input.commits.Load() == 1
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, int32(1), optDLQ.received.Load(), "event must enter the failed branch own dead queue")
	assert.Equal(t, float64(0), destinationResult(t, registry, "archive", "success"))
	assert.Equal(t, float64(1), destinationResult(t, registry, "archive", "deadqueue"))
}

func TestRequiredBranchDeadQueueConverges(t *testing.T) {
	t.Parallel()

	p, input, registry := newBarrierTestPipeline(t)
	req := addControllableOutput(t, p, "archive", true, pipeline.FailurePolicyDiscard)
	dlq := attachControllableDeadQueue(t, p, "archive", "archive_dlq")

	req.onOut = req.fail
	dlq.onOut = dlq.succeed

	p.Start()
	defer p.Stop()

	feedTestEvent(p, 6, 50)

	assert.Eventually(t, func() bool {
		return input.commits.Load() == 1
	}, time.Second, 10*time.Millisecond,
		"position commits only after the dead queue accepted the event")

	assert.Equal(t, float64(0), destinationResult(t, registry, "archive", "success"))
	assert.Equal(t, float64(1), destinationResult(t, registry, "archive", "deadqueue"))
}

func TestHealthIsolation(t *testing.T) {
	t.Parallel()

	p, _, registry := newBarrierTestPipeline(t)
	ok := addControllableOutput(t, p, "search", true, pipeline.FailurePolicyDiscard)
	bad := addControllableOutput(t, p, "archive", true, pipeline.FailurePolicyDiscard)

	ok.onOut = ok.succeed
	bad.onOut = bad.fail

	p.Start()
	defer p.Stop()

	feedTestEvent(p, 7, 60)

	assert.Eventually(t, func() bool {
		return healthGauge(t, registry, "archive") == 0
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, float64(1), healthGauge(t, registry, "search"),
		"one branch failing must not change another branch health")

	bad.succeed(bad.pendingEvent())

	assert.Eventually(t, func() bool {
		return healthGauge(t, registry, "archive") == 1
	}, time.Second, 10*time.Millisecond, "health recovers when the branch later succeeds")
}

func TestBarrierStoreResumeAfterRestart(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "output_offsets.yaml")

	newPipeline := func() (*pipeline.Pipeline, *countingInput, *controllableOutput, *controllableOutput) {
		registry := prometheus.NewRegistry()
		settings := &pipeline.Settings{
			Capacity:            256,
			MaintenanceInterval: time.Hour,
			EventTimeout:        30 * time.Second,
			Antispam: pipeline.AntispamSettings{
				Threshold: pipeline.DefaultAntispamThreshold,
			},
			AvgEventSize:      2048,
			MetaCacheSize:     32,
			StreamField:       "stream",
			Decoder:           "json",
			OutputOffsetsFile: stateFile,
			Metric: &pipeline.MetricSettings{
				HoldDuration: pipeline.DefaultMetricHoldDuration,
			},
		}
		p := pipeline.New("router_test", settings, registry, zap.NewNop())
		p.DisableParallelism()

		input := &countingInput{}
		p.SetInput(&pipeline.InputPluginInfo{
			PluginStaticInfo:  &pipeline.PluginStaticInfo{Type: "counting", Config: struct{}{}},
			PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: input},
		})

		branchA := addControllableOutput(nil, p, "search", true, pipeline.FailurePolicyDiscard)
		branchB := addControllableOutput(nil, p, "archive", true, pipeline.FailurePolicyDiscard)
		return p, input, branchA, branchB
	}

	// first run: branch A converges, branch B holds the event.
	p1, _, a1, b1 := newPipeline()
	a1.onOut = a1.succeed
	p1.Start()
	feedTestEvent(p1, 42, 777)
	require.Eventually(t, func() bool { return b1.received.Load() == 1 }, time.Second, 10*time.Millisecond)
	p1.Stop() // flushes the durable table

	// restart with fresh destination plugins.
	p2, input2, a2, b2 := newPipeline()
	b2.onOut = b2.succeed
	p2.Start()
	feedTestEvent(p2, 42, 777) // input replays the same event

	assert.Eventually(t, func() bool {
		return input2.commits.Load() == 1
	}, time.Second, 10*time.Millisecond,
		"after restart only unfinished destinations must receive the event")

	assert.Equal(t, int32(0), a2.received.Load(), "already converged branch must be skipped after restart")
	assert.Equal(t, int32(1), b2.received.Load())
	p2.Stop()

	b, err := os.ReadFile(stateFile)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "source_id", "progress entry of the completed event must be removed")
}

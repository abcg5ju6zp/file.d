package pipeline_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type fakeOutputPluginController struct {
	mu      sync.Mutex
	commits []*pipeline.Event
	errors  []string
}

func (f *fakeOutputPluginController) Commit(event *pipeline.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, event)
}
func (f *fakeOutputPluginController) Error(err string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors = append(f.errors, err)
}

func (f *fakeOutputPluginController) getCommits() []*pipeline.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits
}

func (f *fakeOutputPluginController) getErrors() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errors
}

func TestRouterNormalProcessing(t *testing.T) {
	t.Parallel()

	r := pipeline.NewRouter()
	controller := &fakeOutputPluginController{}

	// Setup main output that succeeds
	var outputCount atomic.Int32
	outputPlugin, outputConfig := createDevNullPlugin(func(event *pipeline.Event) {
		outputCount.Add(1)
	})

	r.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Type: "devnull", Config: outputConfig},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: outputPlugin},
	})

	// Setup dead queue that shouldn't be used
	var deadQueueCount atomic.Int32
	deadQueuePlugin, deadQueueConfig := createDevNullPlugin(func(event *pipeline.Event) {
		deadQueueCount.Add(1)
	})

	r.SetDeadQueueOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Type: "devnull", Config: deadQueueConfig},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: deadQueuePlugin},
	})

	params := test.NewEmptyOutputPluginParams()
	params.PipelineName = "test_pipeline"
	params.Router = r
	params.Controller = controller
	r.Start(params)
	defer r.Stop()

	// Send test event
	event := newEvent(t)
	r.Out(event)

	// Wait for processing
	assert.Eventually(t, func() bool {
		return outputCount.Load() == 1
	}, 100*time.Millisecond, 10*time.Millisecond, "event should be processed")

	assert.Equal(t, int32(0), deadQueueCount.Load(), "dead queue should not be used")
	assert.Len(t, controller.getCommits(), 1, "should commit successful event")
	assert.Empty(t, controller.getErrors(), "should not produce errors")
}

func TestRouterDeadQueueProcessing(t *testing.T) {
	t.Parallel()

	r := pipeline.NewRouter()
	controller := &fakeOutputPluginController{}

	// Setup main output that fails
	var outputCount atomic.Int32
	outputPlugin, outputConfig := createDevNullPlugin(func(event *pipeline.Event) {
		outputCount.Add(1)
		r.Fail(event, nil)
	})

	r.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Type: "devnull", Config: outputConfig},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: outputPlugin},
	})

	// Setup dead queue
	var deadQueueCount atomic.Int32
	deadQueuePlugin, deadQueueConfig := createDevNullPlugin(func(event *pipeline.Event) {
		deadQueueCount.Add(1)
	})

	r.SetDeadQueueOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Type: "devnull", Config: deadQueueConfig},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: deadQueuePlugin},
	})

	params := test.NewEmptyOutputPluginParams()
	params.PipelineName = "test_pipeline"
	params.Router = r
	params.Controller = controller
	r.Start(params)
	defer r.Stop()

	// Send test event
	event := newEvent(t)
	r.Out(event)

	// Wait for processing
	assert.Eventually(t, func() bool {
		return deadQueueCount.Load() == 1
	}, 100*time.Millisecond, 10*time.Millisecond, "event should go to dead queue")

	assert.Equal(t, int32(1), outputCount.Load(), "main output should try to process")
	assert.Len(t, controller.getCommits(), 2, "should commit successful event")
	assert.Empty(t, controller.getErrors(), "should not produce errors")
}

// scriptedOutput is a controllable output plugin: it can fail or succeed
// and optionally commit after reporting a failure, like the retriable batcher does.
type scriptedOutput struct {
	controller pipeline.OutputPluginController
	router     *pipeline.Router

	mu       sync.Mutex
	outCount int
	events   []*pipeline.Event

	// fail makes Out report a failure to the router instead of committing.
	fail bool
	// commitOnFail makes the plugin commit after reporting a failure
	// (models the terminal commit performed by the failover batcher).
	commitOnFail bool
	// hold makes Out accept the event without any fail or commit
	// (models an in-flight event lost by a process crash).
	hold bool
}

func (s *scriptedOutput) Start(_ pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	s.controller = params.Controller
	s.router = params.Router
}

func (s *scriptedOutput) Stop() {}

func (s *scriptedOutput) Out(event *pipeline.Event) {
	s.mu.Lock()
	s.outCount++
	s.events = append(s.events, event)
	fail, commitOnFail, hold := s.fail, s.commitOnFail, s.hold
	s.mu.Unlock()

	switch {
	case hold:
		return
	case fail:
		s.router.Fail(event, fmt.Errorf("output attempt failed"))
		if commitOnFail {
			s.controller.Commit(event)
		}
	default:
		s.controller.Commit(event)
	}
}

func (s *scriptedOutput) getOutCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outCount
}

func setupRouterWithScripts(t *testing.T) (*pipeline.Router, *fakeOutputPluginController, *scriptedOutput, *scriptedOutput, *observer.ObservedLogs) {
	t.Helper()

	core, recorded := observer.New(zapcore.InfoLevel)

	r := pipeline.NewRouter()
	controller := &fakeOutputPluginController{}

	main := &scriptedOutput{fail: true}
	failover := &scriptedOutput{}

	r.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Type: "main_plugin"},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: main},
	})
	r.SetDeadQueueOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Type: "failover_plugin"},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: failover},
	})

	params := test.NewEmptyOutputPluginParams()
	params.Controller = controller
	params.Logger = zap.New(core).Sugar()
	r.Start(params)
	t.Cleanup(r.Stop)

	return r, controller, main, failover, recorded
}

func newSourcedEvent(t *testing.T, offset int64) *pipeline.Event {
	t.Helper()
	e := newEvent(t)
	e.SourceID = pipeline.SourceID(42)
	e.SourceName = "source-file.log"
	e.Offset = offset
	return e
}

func fieldValue(entries []observer.LoggedEntry, key string) (zapcore.Field, bool) {
	for _, e := range entries {
		for _, f := range e.Context {
			if f.Key == key {
				return f, true
			}
		}
	}
	return zapcore.Field{}, false
}

// Main output fails once, failover output succeeds: exactly one transfer and one commit,
// and the failover output receives the event with its original source preserved.
func TestRouterFailoverThenSuccess(t *testing.T) {
	t.Parallel()

	r, controller, main, failover, recorded := setupRouterWithScripts(t)

	event := newSourcedEvent(t, 100500)
	r.Out(event)

	assert.Eventually(t, func() bool {
		return failover.getOutCount() == 1
	}, time.Second, 10*time.Millisecond, "event should be delivered to the failover output")

	assert.Equal(t, 1, main.getOutCount(), "main output should process once")
	assert.Len(t, controller.getCommits(), 1, "event should be committed only after the failover success")

	transfers := recorded.FilterMessageSnippet("transferring event to the failover output").All()
	require.Len(t, transfers, 1, "transfer should be logged once")

	f, ok := fieldValue(transfers, "route")
	require.True(t, ok, "route field should be logged")
	assert.Equal(t, "failover", f.String)

	for key, want := range map[string]string{
		"main_output":     "main_plugin",
		"failover_output": "failover_plugin",
		"source_name":     "source-file.log",
	} {
		f, ok := fieldValue(transfers, key)
		require.True(t, ok, "%s field should be logged", key)
		assert.Equal(t, want, f.String)
	}

	f, ok = fieldValue(transfers, "offset")
	require.True(t, ok, "offset field should be logged")
	assert.Equal(t, int64(100500), f.Integer)

	// the failover output keeps the original source
	delivered := failover.events
	require.Len(t, delivered, 1)
	assert.Equal(t, pipeline.OutputRouteFailover, delivered[0].Route, "event should carry the failover mark")
	assert.Equal(t, pipeline.SourceID(42), delivered[0].SourceID)
	assert.Equal(t, "source-file.log", delivered[0].SourceName)
	assert.Equal(t, int64(100500), delivered[0].Offset)
}

// Failover output fails too: the event reaches the terminal state — it's not re-routed
// (no recursion), committed exactly once, and the terminal log contains actionable data.
func TestRouterFailoverFailureTerminal(t *testing.T) {
	t.Parallel()

	r, controller, main, failover, recorded := setupRouterWithScripts(t)
	// simulate the failover batcher terminal commit after reporting the failure
	failover.fail = true
	failover.commitOnFail = true

	event := newSourcedEvent(t, 200600)
	r.Out(event)

	assert.Eventually(t, func() bool {
		return len(controller.getCommits()) == 1
	}, time.Second, 10*time.Millisecond, "terminal event should be committed")

	assert.Equal(t, 1, main.getOutCount(), "main output should process once")
	assert.Equal(t, 1, failover.getOutCount(), "failover output should process once, no recursion")
	assert.Len(t, controller.getCommits(), 1)

	terminal := recorded.FilterMessageSnippet("terminal state").All()
	require.Len(t, terminal, 1, "terminal state should be logged once")

	assert.Len(t, recorded.FilterMessageSnippet("transferring event to the failover output").All(), 1)

	for key, want := range map[string]string{
		"event":       `{}`,
		"source_name": "source-file.log",
	} {
		f, ok := fieldValue(terminal, key)
		require.True(t, ok, "%s field should be logged", key)
		assert.Equal(t, want, f.String, "wrong value for %s", key)
	}

	for key, want := range map[string]string{
		"main_output_error":     "output attempt failed",
		"failover_output_error": "output attempt failed",
	} {
		f, ok := fieldValue(terminal, key)
		require.True(t, ok, "%s field should be logged", key)
		err, ok := f.Interface.(error)
		require.True(t, ok, "%s field should carry an error", key)
		assert.Equal(t, want, err.Error(), "wrong value for %s", key)
	}

	f, ok := fieldValue(terminal, "offset")
	require.True(t, ok, "offset field should be logged")
	assert.Equal(t, int64(200600), f.Integer)
}

// Process restart: while the event travels the failover path nothing is committed,
// so the offset isn't persisted. On restart the input replays only such uncommitted
// events, which re-enter through the main output; committed events are skipped.
func TestRouterRestartReplaysUncommitted(t *testing.T) {
	t.Parallel()

	r, controller, main, failover, _ := setupRouterWithScripts(t)
	// failover receives the event but crashes before committing it
	failover.hold = true

	event := newSourcedEvent(t, 300700)
	r.Out(event)

	assert.Eventually(t, func() bool {
		return failover.getOutCount() == 1
	}, time.Second, 10*time.Millisecond, "event should reach the failover output")
	assert.Empty(t, controller.getCommits(), "uncommitted event keeps the input offset")

	// restart: input replays the uncommitted offset, event re-enters the (recovered) main output
	main.fail = false
	replayed := newSourcedEvent(t, 300700)
	r.Out(replayed)

	assert.Eventually(t, func() bool {
		return len(controller.getCommits()) == 1
	}, time.Second, 10*time.Millisecond, "replayed event should be committed")
	assert.Equal(t, pipeline.OutputRouteMain, replayed.Route, "replayed event starts from the main output")
	assert.Equal(t, 2, main.getOutCount(), "main output should process the replayed event")
}

func createDevNullPlugin(outFn func(event *pipeline.Event)) (*devnull.Plugin, pipeline.AnyConfig) {
	plugin, config := devnull.Factory()
	p := plugin.(*devnull.Plugin)
	p.SetOutFn(outFn)
	return p, config
}

func newEvent(t *testing.T) *pipeline.Event {
	root, err := insaneJSON.DecodeString(`{}`)
	if err != nil {
		t.Skip() // ignore invalid input
	}
	return &pipeline.Event{
		Root: root,
		Buf:  make([]byte, 0, 1024),
	}
}

package fd

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type outputsTestInput struct{}

func (*outputsTestInput) Start(pipeline.AnyConfig, *pipeline.InputPluginParams) {}
func (*outputsTestInput) Stop()                                                 {}
func (*outputsTestInput) Commit(*pipeline.Event)                                {}
func (*outputsTestInput) PassEvent(*pipeline.Event) bool                        { return true }

// testOutputPlugin is a trivial output registered locally to avoid plugin import cycles.
type testOutputPlugin struct{}
type testOutputConfig struct{}

func (*testOutputPlugin) Start(pipeline.AnyConfig, *pipeline.OutputPluginParams) {}
func (*testOutputPlugin) Stop()                                                  {}
func (*testOutputPlugin) Out(*pipeline.Event)                                    {}

// batcherTestPlugin is an output with a real batcher, used for metrics isolation checks.
type batcherTestPlugin struct {
	batcher *pipeline.Batcher
}
type batcherTestConfig struct{}

func (p *batcherTestPlugin) Start(_ pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	opts := pipeline.BatcherOptions{
		PipelineName:   "outputs_test",
		OutputType:     "test_batcher",
		OutFn:          func(*pipeline.WorkerData, *pipeline.Batch) {},
		Controller:     params.Controller,
		Workers:        1,
		BatchSizeCount: 1,
		FlushTimeout:   time.Hour,
		MetricCtl:      params.MetricCtl,
	}
	p.batcher = pipeline.NewBatcher(opts)
	p.batcher.Start(context.Background())
}

func (p *batcherTestPlugin) Stop()                     { p.batcher.Stop() }
func (p *batcherTestPlugin) Out(event *pipeline.Event) { p.batcher.Add(event) }

func init() {
	DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type: "test_output",
		Factory: func() (pipeline.AnyPlugin, pipeline.AnyConfig) {
			return &testOutputPlugin{}, &testOutputConfig{}
		},
	})
	DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type: "test_batcher",
		Factory: func() (pipeline.AnyPlugin, pipeline.AnyConfig) {
			return &batcherTestPlugin{}, &batcherTestConfig{}
		},
	})
}

func newOutputsTestPipeline() *pipeline.Pipeline {
	settings := &pipeline.Settings{
		Capacity:            64,
		MaintenanceInterval: time.Hour,
		EventTimeout:        time.Second,
		AvgEventSize:        1024,
		Decoder:             "json",
		OutputOffsetsFile:   pipeline.OffOutputOffsets,
		Metric: &pipeline.MetricSettings{
			HoldDuration: time.Minute,
		},
	}
	return pipeline.New("outputs_test", settings, prometheus.NewRegistry(), zap.NewNop())
}

func runOutputsSetup(t *testing.T, jsonCfg string) (*FileD, *pipeline.Pipeline, error) {
	t.Helper()

	f := New(cfg.NewConfig(), "")
	f.createRegistry()

	raw, err := simplejson.NewJson([]byte(jsonCfg))
	require.NoError(t, err)

	p := newOutputsTestPipeline()
	values := map[string]int{"capacity": 64, "gomaxprocs": 8}
	err = f.setupOutputs(p, &cfg.PipelineConfig{Raw: raw}, values)
	return f, p, err
}

func TestOutputsConfigValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		jsonCfg string
	}{
		{"missing both", `{}`},
		{"both configured", `{"output":{"type":"test_output"},"outputs":[{"name":"a","type":"test_output"}]}`},
		{"empty list", `{"outputs":[]}`},
		{"missing name", `{"outputs":[{"type":"test_output"}]}`},
		{"invalid name", `{"outputs":[{"name":"bad-name","type":"test_output"}]}`},
		{"duplicate names", `{"outputs":[{"name":"a","type":"test_output"},{"name":"a","type":"test_output"}]}`},
		{"missing type", `{"outputs":[{"name":"a"}]}`},
		{"invalid on_failure", `{"outputs":[{"name":"a","type":"test_output","on_failure":"oops"}]}`},
		{"deadqueue policy without deadqueue", `{"outputs":[{"name":"a","type":"test_output","on_failure":"deadqueue"}]}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := runOutputsSetup(t, tc.jsonCfg)
			assert.Error(t, err)
		})
	}
}

func TestOutputsConfigValid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		jsonCfg string
	}{
		{"valid single output", `{"output":{"type":"test_output"}}`},
		{"valid list", `{"outputs":[{"name":"a","type":"test_output","required":false}]}`},
		{
			"valid list with deadqueue policy",
			`{"outputs":[{"name":"a","type":"test_output","required":false,"on_failure":"deadqueue","deadqueue":{"type":"test_output"}}]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := runOutputsSetup(t, tc.jsonCfg)
			assert.NoError(t, err)
		})
	}
}

func TestOutputsExceedsMax(t *testing.T) {
	t.Parallel()

	names := make([]string, 0, pipeline.MaxOutputCount+1)
	for i := range pipeline.MaxOutputCount + 1 {
		names = append(names, fmt.Sprintf("o%d", i))
	}

	var sb strings.Builder
	sb.WriteString(`{"outputs":[`)
	for i, n := range names {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"name":"` + n + `","type":"test_output"}`)
	}
	sb.WriteString(`]}`)

	_, _, err := runOutputsSetup(t, sb.String())
	assert.ErrorContains(t, err, "exceeds max")
}

func TestMetricsIsolationByDestination(t *testing.T) {
	t.Parallel()

	jsonCfg := `{
		"outputs": [
			{"name":"a","type":"test_batcher"},
			{"name":"b","type":"test_batcher"}
		]
	}`

	f, p, err := runOutputsSetup(t, jsonCfg)
	require.NoError(t, err)

	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:   "test",
			Config: struct{}{},
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: &outputsTestInput{},
		},
	})

	p.Start()

	mfs, err := f.registry.Gather()
	require.NoError(t, err)

	hasFamily := func(name string) bool {
		for _, mf := range mfs {
			if mf.GetName() == name {
				return true
			}
		}
		return false
	}

	assert.True(t, hasFamily("file_d_pipeline_outputs_test_output_a_batcher_jobs_done_total"),
		"branch a must have its own batcher metrics family")
	assert.True(t, hasFamily("file_d_pipeline_outputs_test_output_b_batcher_jobs_done_total"),
		"branch b must have its own batcher metrics family")

	p.Stop()
}

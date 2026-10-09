package fd_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	_ "github.com/ozontech/file.d/plugin/action/discard"
	"github.com/ozontech/file.d/plugin/input/fake"
	_ "github.com/ozontech/file.d/plugin/input/socket"
	_ "github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	k8syaml "sigs.k8s.io/yaml"
)

func newCfg(t *testing.T, c map[string]string) *cfg.Config {
	t.Helper()
	conf := cfg.NewConfig()
	for name, s := range c {
		var obj any
		require.NoError(t, k8syaml.Unmarshal([]byte(s), &obj))
		b, err := json.Marshal(obj)
		require.NoError(t, err)
		j, err := simplejson.NewJson(b)
		require.NoError(t, err)
		conf.Pipelines[name] = &cfg.PipelineConfig{Raw: j}
	}
	return conf
}

const p1FakeDevnull = `
input:
  type: fake
output:
  type: devnull
`

const p1Discard = `
input:
  type: fake
actions:
  - type: discard
output:
  type: devnull
`

const p1BadAction = `
input:
  type: fake
actions:
  - type: no_such_plugin_xyz
output:
  type: devnull
`

func sendTestEvent(p *pipeline.Pipeline, id pipeline.SourceID) {
	in := p.GetInput().(*fake.Plugin)
	in.In(id, "test", pipeline.NewOffsets(int64(id), nil), []byte(`{"a":"b"}`))
}

func waitCommitted(t *testing.T, c *atomic.Int64, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return c.Load() == want
	}, time.Second, time.Millisecond*5)
}

func TestReloadBuildFailureKeepsOldGeneration(t *testing.T) {
	f := fd.New(newCfg(t, map[string]string{"p1": p1FakeDevnull}), "off")
	f.Start()
	defer func() { _ = f.Stop(context.Background()) }()

	old := f.GetPipeline("p1")
	require.NotNil(t, old)

	var committed atomic.Int64
	old.GetInput().(*fake.Plugin).SetCommitFn(func(*pipeline.Event) { committed.Inc() })

	sendTestEvent(old, 1)
	waitCommitted(t, &committed, 1)

	err := f.Reload(newCfg(t, map[string]string{"p1": p1BadAction}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "no_such_plugin_xyz")

	// the old pipeline keeps serving
	sendTestEvent(old, 2)
	waitCommitted(t, &committed, 2)
	require.Same(t, old, f.GetPipeline("p1"))
	require.Equal(t, uint64(1), f.ActiveGenerationID())
}

func TestReloadListenAddressConflict(t *testing.T) {
	f := fd.New(newCfg(t, map[string]string{"p1": p1FakeDevnull}), "off")
	f.Start()
	defer func() { _ = f.Stop(context.Background()) }()

	old := f.GetPipeline("p1")
	var committed atomic.Int64
	old.GetInput().(*fake.Plugin).SetCommitFn(func(*pipeline.Event) { committed.Inc() })

	conflicting := newCfg(t, map[string]string{
		"p1": `
input:
  type: socket
  network: tcp
  address: 127.0.0.1:19131
output:
  type: devnull
`,
		"p2": `
input:
  type: socket
  network: tcp
  address: 127.0.0.1:19131
output:
  type: devnull
`,
	})

	err := f.Reload(conflicting)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already used")

	// old generation intact and serving
	sendTestEvent(old, 5)
	waitCommitted(t, &committed, 1)
	require.Same(t, old, f.GetPipeline("p1"))
}

func TestReloadNoChangeIsNoop(t *testing.T) {
	f := fd.New(newCfg(t, map[string]string{"p1": p1FakeDevnull}), "off")
	f.Start()
	defer func() { _ = f.Stop(context.Background()) }()

	p := f.GetPipeline("p1")

	// a separately parsed but identical config → same hashes
	require.NoError(t, f.Reload(newCfg(t, map[string]string{"p1": p1FakeDevnull})))
	require.Same(t, p, f.GetPipeline("p1"))
}

func TestReloadChangedPipelineDrainsAndSwitches(t *testing.T) {
	f := fd.New(newCfg(t, map[string]string{"p1": p1FakeDevnull}), "off")
	f.Start()
	defer func() { _ = f.Stop(context.Background()) }()

	old := f.GetPipeline("p1")
	var committed atomic.Int64
	old.GetInput().(*fake.Plugin).SetCommitFn(func(*pipeline.Event) { committed.Inc() })

	// send an event right at the reload edge; the old pipeline must
	// stop ingress and drain it before the switch completes.
	sendTestEvent(old, 10)

	require.NoError(t, f.Reload(newCfg(t, map[string]string{"p1": p1Discard})))

	// drained in-flight event was committed
	waitCommitted(t, &committed, 1)

	newP := f.GetPipeline("p1")
	require.NotSame(t, old, newP)
	require.Equal(t, uint64(2), f.ActiveGenerationID())

	// the new pipeline discards events, no new commits
	sendTestEvent(newP, 11)
	require.Never(t, func() bool { return committed.Load() != 1 }, time.Millisecond*300, time.Millisecond*20)
}

func TestReloadConcurrentAreSerialized(t *testing.T) {
	f := fd.New(newCfg(t, map[string]string{"p1": p1FakeDevnull}), "off")
	f.Start()
	defer func() { _ = f.Stop(context.Background()) }()

	const n = 12
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var conf *cfg.Config
			if i%2 == 0 {
				conf = newCfg(t, map[string]string{"p1": p1Discard})
			} else {
				conf = newCfg(t, map[string]string{"p1": p1FakeDevnull})
			}
			require.NoError(t, f.Reload(conf))
		}(i)
	}
	wg.Wait()

	// exactly one consistent active generation and pipeline;
	// reloads that saw an identical active config are no-ops,
	// so the generation id is bounded but not exactly n.
	p := f.GetPipeline("p1")
	require.NotNil(t, p)
	id := f.ActiveGenerationID()
	require.GreaterOrEqual(t, id, uint64(1))
	require.LessOrEqual(t, id, uint64(1+n))
}

func TestReloadStatusEndpoint(t *testing.T) {
	f := fd.New(newCfg(t, map[string]string{"p1": p1FakeDevnull}), "127.0.0.1:19155")
	f.Start()

	ready := func() int {
		resp, err := http.Get("http://127.0.0.1:19155/ready")
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	require.Equal(t, http.StatusOK, ready())

	require.Error(t, f.Reload(newCfg(t, map[string]string{"p1": p1BadAction})))

	// failure reason is exposed while the old generation still serves
	resp, err := http.Get("http://127.0.0.1:19155/reload/status")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Contains(t, string(body), "no_such_plugin_xyz")
	require.Contains(t, string(body), `"success":false`)

	require.Equal(t, http.StatusOK, ready())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, f.Stop(ctx))
}

package fd

import (
	"net/http"

	"github.com/ozontech/file.d/buildinfo"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/atomic"
)

// pipelineEntry binds a pipeline object of the generation
// to its name and the hash of its effective config.
type pipelineEntry struct {
	name     string
	hash     string
	pipeline *pipeline.Pipeline
}

// generation is one definite configuration epoch:
// config snapshot, own prometheus registry, own HTTP mux and pipelines.
// HTTP routes, readiness and metrics of a generation never mix with
// another generation; the active generation is switched atomically.
type generation struct {
	id     uint64
	config *cfg.Config
	hash   string

	registry *prometheus.Registry
	mux      *http.ServeMux
	ready    atomic.Bool

	entries []*pipelineEntry // ordered by pipeline name

	// file_d level metrics
	metricCtl     *metric.Ctl
	versionMetric *metric.GaugeVec
}

func newGeneration(id uint64, config *cfg.Config) *generation {
	return &generation{
		id:     id,
		config: config,
		mux:    http.NewServeMux(),
	}
}

func (g *generation) init() {
	g.createRegistry()

	g.metricCtl = metric.NewCtl("file_d", g.registry, 0, 0)
	g.versionMetric = g.metricCtl.RegisterGaugeVec("version", "", "version")
	g.versionMetric.WithLabelValues(buildinfo.Version).Inc()

	g.mux.HandleFunc("/ready", g.serveReady)
	g.mux.Handle("/metrics", promhttp.InstrumentMetricHandler(
		g.registry, promhttp.HandlerFor(g.registry, promhttp.HandlerOpts{}),
	))
}

func (g *generation) createRegistry() {
	g.registry = prometheus.NewRegistry()
	g.registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	g.registry.MustRegister(prometheus.NewGoCollector())
	g.registry.MustRegister(newFdsCollector())
}

func (g *generation) serveReady(w http.ResponseWriter, _ *http.Request) {
	if !g.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (g *generation) entriesByName() map[string]*pipelineEntry {
	m := make(map[string]*pipelineEntry, len(g.entries))
	for _, e := range g.entries {
		m[e.name] = e
	}
	return m
}

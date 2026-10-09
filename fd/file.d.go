package fd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/cespare/xxhash/v2"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/atomic"
)

// defaultDrainTimeout bounds in-flight event draining of the old
// pipelines during a generation switch.
const defaultDrainTimeout = 10 * time.Second

type FileD struct {
	config   *cfg.Config
	httpAddr string
	plugins  *PluginRegistry

	// management HTTP server is process-persistent: SIGHUP never stops it,
	// so there is no rebind/conflict window. rootMux delegates generation
	// dependent requests to the active generation.
	server   *http.Server
	rootMux  *http.ServeMux
	stopChan chan struct{}

	// activeGen is the one definite serving generation; it is switched
	// by a single atomic store, so routes/readiness/metrics can never mix.
	activeGen atomic.Pointer[generation]
	genID     atomic.Uint64

	// reloadMu serializes SIGHUP reloads and also excludes Stop from a switch.
	reloadMu sync.Mutex

	// retiring pipelines stopped receiving events and drain in background.
	retiringMu sync.Mutex
	retiring   []*pipeline.Pipeline

	statusMu     sync.RWMutex
	reloadStatus reloadStatus
}

// reloadStatus is exposed via GET /reload/status and provides the last
// reload result, including the failure reason.
type reloadStatus struct {
	Generation uint64    `json:"generation"`
	UpdatedAt  time.Time `json:"updated_at"`
	Success    bool      `json:"success"`
	Error      string    `json:"error,omitempty"`
}

func New(config *cfg.Config, httpAddr string) *FileD {
	return &FileD{
		config:   config,
		httpAddr: httpAddr,
		plugins:  DefaultPluginRegistry,
	}
}

func (f *FileD) SetConfig(config *cfg.Config) {
	f.config = config
}

func (f *FileD) nextGenID() uint64 {
	return f.genID.Add(1)
}

func (f *FileD) Start() {
	logger.Infof("starting file.d")

	f.startHTTP()

	perHashes, fullHash, err := configHashes(f.config)
	if err != nil {
		logger.Fatalf("%s", err.Error())
	}

	gen, err := f.buildGeneration(f.nextGenID(), f.config, nil, perHashes, fullHash)
	if err != nil {
		logger.Fatalf("%s", err.Error())
	}

	f.startGeneration(gen)
	f.activeGen.Store(gen)
	f.setReloadStatus(gen.id, true, nil)
}

func (f *FileD) startGeneration(gen *generation) {
	for _, e := range gen.entries {
		if err := e.pipeline.Start(); err != nil {
			logger.Fatalf("can't start pipeline %q: %s", e.name, err.Error())
		}
	}
	gen.ready.Store(true)
}

// GetPipeline returns the pipeline of the active generation by name.
func (f *FileD) GetPipeline(name string) *pipeline.Pipeline {
	if gen := f.activeGen.Load(); gen != nil {
		if e, ok := gen.entriesByName()[name]; ok {
			return e.pipeline
		}
	}
	return nil
}

// ActiveGenerationID returns the id of the active generation.
func (f *FileD) ActiveGenerationID() uint64 {
	if gen := f.activeGen.Load(); gen != nil {
		return gen.id
	}
	return 0
}

// Reload applies new config with the generation switch.
//
// The candidate generation is fully parsed, validated and built
// (standby inputs bind their listeners) before the active generation
// is touched. On any build failure the active generation keeps serving
// and the failure reason is exposed via /reload/status.
// Calls are serialized.
func (f *FileD) Reload(newConfig *cfg.Config) error {
	f.reloadMu.Lock()
	defer f.reloadMu.Unlock()

	active := f.activeGen.Load()

	perHashes, fullHash, err := configHashes(newConfig)
	if err != nil {
		f.setReloadStatus(active.id, false, err)
		return err
	}

	if fullHash == active.hash {
		logger.Infof("config unchanged, reload skipped")
		f.setReloadStatus(active.id, true, nil)
		return nil
	}

	// unchanged pipelines reuse the running pipeline object: input
	// positions and in-flight semantics are preserved.
	oldByName := active.entriesByName()
	reuse := make(map[string]*pipeline.Pipeline)
	for name, h := range perHashes {
		if old, ok := oldByName[name]; ok && old.hash == h {
			reuse[name] = old.pipeline
		}
	}

	cand, err := f.buildGeneration(f.nextGenID(), newConfig, reuse, perHashes, fullHash)
	if err != nil {
		f.setReloadStatus(active.id, false, err)
		return err
	}

	if err := f.switchOver(active, cand); err != nil {
		f.setReloadStatus(active.id, false, err)
		return err
	}

	f.config = newConfig
	f.setReloadStatus(cand.id, true, nil)
	return nil
}

// buildGeneration constructs one generation. Only pipelines missing from
// reuse are built; reused pipeline objects get their existing collectors
// and HTTP handlers carried into the new generation.
func (f *FileD) buildGeneration(
	id uint64,
	config *cfg.Config,
	reuse map[string]*pipeline.Pipeline,
	perHashes map[string]string,
	fullHash string,
) (_ *generation, err error) {
	gen := newGeneration(id, config)
	gen.init()
	gen.hash = fullHash

	defer func() {
		if err != nil {
			// release resources acquired during the partial build;
			// reused pipelines aren't touched.
			for _, e := range gen.entries {
				if _, isReused := reuse[e.name]; !isReused {
					e.pipeline.AbortPrepared()
				}
			}
		}
	}()

	names := make([]string, 0, len(config.Pipelines))
	for name := range config.Pipelines {
		names = append(names, name)
	}
	sort.Strings(names)

	// resource conflicts are validated over pristine Raw configs BEFORE
	// any pipeline is built (the build mutates Raw, e.g. deletes the
	// input "type"). On failure nothing is constructed at all.
	if err = f.validateGeneration(config, names); err != nil {
		return nil, err
	}

	for _, name := range names {
		hash := perHashes[name]

		if p, ok := reuse[name]; ok {
			for _, c := range p.MetricCollectors() {
				if regErr := gen.registry.Register(c); regErr != nil {
					return nil, fmt.Errorf("can't register metrics of unchanged pipeline %q: %w", name, regErr)
				}
			}
			// expose the same pipeline under the new generation routes
			p.SetupHTTPHandlers(gen.mux)
			gen.entries = append(gen.entries, &pipelineEntry{name: name, hash: hash, pipeline: p})
			continue
		}

		if buildErr := f.addPipeline(gen, name, config.Pipelines[name], hash); buildErr != nil {
			return nil, buildErr
		}
	}

	// standby inputs acquire their resources (bind listeners) while
	// the active generation keeps serving.
	for _, e := range gen.entries {
		if _, isReused := reuse[e.name]; isReused {
			continue
		}
		standby, ok := e.pipeline.GetInput().(pipeline.InputPluginStandby)
		if !ok {
			continue
		}
		if err = standby.Prepare(e.pipeline.InputConfig(), e.pipeline.InputParams()); err != nil {
			return nil, fmt.Errorf("can't prepare input for pipeline %q: %w", e.name, err)
		}
	}

	return gen, nil
}

// validateGeneration rejects resource conflicts within the effective
// pipeline set: duplicated listen endpoints and offsets files.
// It runs before the Raw configs are mutated by the pipeline build.
func (f *FileD) validateGeneration(config *cfg.Config, names []string) error {
	listen := make(map[string]string)
	offsets := make(map[string]string)

	for _, name := range names {
		raw := config.Pipelines[name].Raw

		if key, isListen := listenResource(raw); isListen {
			if other, dup := listen[key]; dup {
				return fmt.Errorf("pipeline %q: listen %s is already used by pipeline %q", name, key, other)
			}
			listen[key] = name
		}

		if of := raw.Get("input").Get("offsets_file").MustString(); of != "" {
			if other, dup := offsets[of]; dup {
				return fmt.Errorf("pipeline %q: offsets file %q is already used by pipeline %q", name, of, other)
			}
			offsets[of] = name
		}
	}
	return nil
}

// listenResource reports the process-global listening resource of a pipeline, if any.
func listenResource(raw *simplejson.Json) (string, bool) {
	input := raw.Get("input")
	switch input.Get("type").MustString() {
	case "socket":
		addr := input.Get("address").MustString()
		if addr == "" {
			return "", false
		}
		switch input.Get("network").MustString("tcp") {
		case "udp":
			return "udp:" + addr, true
		case "unix":
			return "unix:" + addr, true
		default:
			return "tcp:" + addr, true
		}
	case "http":
		addr := input.Get("address").MustString()
		if addr == "" || addr == "off" {
			return "", false
		}
		return "tcp:" + addr, true
	}
	return "", false
}

// configHashes computes per-pipeline hashes (pristine Raw) and the
// aggregate config hash; stable (names sorted, JSON map keys sorted).
func configHashes(config *cfg.Config) (map[string]string, string, error) {
	per := make(map[string]string, len(config.Pipelines))

	names := make([]string, 0, len(config.Pipelines))
	for name := range config.Pipelines {
		names = append(names, name)
	}
	sort.Strings(names)

	h := xxhash.New()
	for _, name := range names {
		b, err := config.Pipelines[name].Raw.Encode()
		if err != nil {
			return nil, "", fmt.Errorf("can't encode pipeline %q config: %w", name, err)
		}
		per[name] = fmt.Sprintf("%x", xxhash.Sum64(b))
		_, _ = h.Write([]byte(name))
		_, _ = h.Write(b)
	}
	return per, fmt.Sprintf("%x", h.Sum(nil)), nil
}

// switchOver starts the candidate pipelines and retires old changed/removed
// pipelines. Non-standby old pipelines are fully drained before their
// successors start (they may share persistent state such as offsets files);
// standby successors start immediately and their old pipelines drain in
// background.
func (f *FileD) switchOver(oldGen, cand *generation) error {
	oldByName := oldGen.entriesByName()
	candByName := cand.entriesByName()

	// old pipelines that leave the active set: changed or removed
	var stopping []*pipelineEntry
	standbyOld := make(map[string]*pipelineEntry)
	for name, e := range oldByName {
		newE, ok := candByName[name]
		if ok && newE.pipeline == e.pipeline {
			continue // unchanged, reused
		}
		stopping = append(stopping, e)
		if _, isStandby := e.pipeline.GetInput().(pipeline.InputPluginStandby); isStandby {
			standbyOld[name] = e
		}
	}

	// 1. activate standby successors BEFORE old listeners are closed:
	// on failure abort the started candidates, the old generation is
	// untouched and keeps serving.
	var startedStandby []*pipeline.Pipeline
	abortStarted := func() {
		for _, p := range startedStandby {
			p.Stop()
		}
	}
	for _, e := range cand.entries {
		if _, isStandby := e.pipeline.GetInput().(pipeline.InputPluginStandby); !isStandby {
			continue
		}
		if oldE, existed := oldByName[e.name]; existed && oldE.pipeline == e.pipeline {
			continue // reused
		}
		if err := e.pipeline.Start(); err != nil {
			abortStarted()
			return fmt.Errorf("can't start pipeline %q: %w", e.name, err)
		}
		startedStandby = append(startedStandby, e.pipeline)
	}

	// 2. close ingress of old standby pipelines (successor already serving)
	for _, e := range standbyOld {
		e.pipeline.StopInput()
		if rh, ok := e.pipeline.GetInput().(pipeline.InputPluginResourceHook); ok {
			rh.Release()
		}
	}

	// 3. non-standby old pipelines: stop ingress, release globals,
	// and drain before their successors start (shared persistent state).
	drainCtx, cancel := context.WithTimeout(context.Background(), defaultDrainTimeout)
	defer cancel()
	for _, e := range stopping {
		if _, isStandby := standbyOld[e.name]; isStandby {
			continue
		}
		e.pipeline.StopInput()
		if rh, ok := e.pipeline.GetInput().(pipeline.InputPluginResourceHook); ok {
			rh.Release()
		}
		_ = e.pipeline.Drain(drainCtx)
	}

	// 4. start non-standby candidate pipelines
	for _, e := range cand.entries {
		oldE, existed := oldByName[e.name]
		if existed && oldE.pipeline == e.pipeline {
			continue // reused
		}
		if _, isStandby := e.pipeline.GetInput().(pipeline.InputPluginStandby); isStandby {
			continue // already started
		}
		// Acquire is handled inside (*Pipeline).Start via the resource hook.
		if err := e.pipeline.Start(); err != nil {
			abortStarted()
			return fmt.Errorf("can't start pipeline %q: %w", e.name, err)
		}
	}

	// 5. old standby pipelines drain in background
	for _, e := range standbyOld {
		f.retire(e.pipeline)
	}

	// 6. single atomic cut: routes/readiness/metrics switch together
	cand.ready.Store(true)
	f.activeGen.Store(cand)

	return nil
}

func (f *FileD) retire(p *pipeline.Pipeline) {
	f.retiringMu.Lock()
	f.retiring = append(f.retiring, p)
	f.retiringMu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultDrainTimeout)
		defer cancel()
		_ = p.Drain(ctx)
	}()
}

func (f *FileD) addPipeline(gen *generation, name string, config *cfg.PipelineConfig, hash string) error {
	settings, err := extractPipelineParams(config.Raw.Get("settings"))
	if err != nil {
		return fmt.Errorf("can't create pipeline %q: %w", name, err)
	}

	values := map[string]int{
		"capacity":   settings.Capacity,
		"gomaxprocs": runtime.GOMAXPROCS(0),
	}

	logger.Infof("creating pipeline %q: capacity=%d, stream field=%s, decoder=%s",
		name, settings.Capacity, settings.StreamField, settings.Decoder)

	if settings.Pool == pipeline.PoolTypeLowMem {
		insaneJSON.StartNodePoolSize = 16
	}

	p, err := pipeline.New(name, settings, gen.registry, logger.Instance.Named(name).Desugar())
	if err != nil {
		return err
	}

	if err := f.setupInput(p, config, values); err != nil {
		return fmt.Errorf("can't create pipeline %q: %w", name, err)
	}

	if err := SetupActions(p, f.plugins, config.Raw.Get("actions"), values); err != nil {
		return fmt.Errorf("can't create pipeline %q: %w", name, err)
	}

	if err := f.setupOutput(p, config, values); err != nil {
		return fmt.Errorf("can't create pipeline %q: %w", name, err)
	}

	p.SetupHTTPHandlers(gen.mux)
	gen.entries = append(gen.entries, &pipelineEntry{name: name, hash: hash, pipeline: p})
	return nil
}

func (f *FileD) setupInput(p *pipeline.Pipeline, pipelineConfig *cfg.PipelineConfig, values map[string]int) error {
	inputInfo, err := f.getStaticInfo(pipelineConfig, pipeline.PluginKindInput, values)
	if err != nil {
		return err
	}

	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo:  inputInfo,
		PluginRuntimeInfo: f.instantiatePlugin(inputInfo),
	})

	for _, actionType := range inputInfo.AdditionalActions {
		actionInfo, err := f.plugins.GetActionByType(actionType)
		if err != nil {
			return err
		}

		infoCopy := *actionInfo
		infoCopy.Config = inputInfo.Config
		infoCopy.Type = actionType

		p.AddAction(&pipeline.ActionPluginStaticInfo{
			PluginStaticInfo: &infoCopy,
			MatchConditions:  pipeline.MatchConditions{},
		})
	}

	return nil
}

func SetupActions(p *pipeline.Pipeline, plugins *PluginRegistry, actions *simplejson.Json, values map[string]int) error {
	for index := range actions.MustArray() {
		actionJSON := actions.GetIndex(index)
		if actionJSON.MustMap() == nil {
			return fmt.Errorf("empty action #%d for pipeline %q", index, p.Name)
		}

		t := actionJSON.Get("type").MustString()
		if t == "" {
			return fmt.Errorf("action #%d doesn't provide type %q", index, p.Name)
		}
		if err := setupAction(p, plugins, index, t, actionJSON, values); err != nil {
			return err
		}
	}
	return nil
}

func setupAction(p *pipeline.Pipeline, plugins *PluginRegistry, index int, t string, actionJSON *simplejson.Json, values map[string]int) error {
	info, err := plugins.GetActionByType(t)
	if err != nil {
		return err
	}

	doIfChecker, err := extractDoIfChecker(actionJSON.Get("do_if"))
	if err != nil {
		return fmt.Errorf(`failed to extract "do_if" conditions for action %d/%s in pipeline %q: %w`, index, t, p.Name, err)
	}

	matchMode := extractMatchMode(actionJSON)
	if matchMode == pipeline.MatchModeUnknown {
		return fmt.Errorf("unknown match_mode value for action %d/%s", index, t)
	}
	matchInvert := extractMatchInvert(actionJSON)
	conditions, err := extractConditions(actionJSON.Get("match_fields"))
	if err != nil {
		return fmt.Errorf("can't extract conditions for action %d/%s: %s", index, t, err.Error())
	}
	metricName, metricLabels, skipStatus := extractMetrics(actionJSON)
	configJSON := makeActionJSON(actionJSON)
	config, err := pipeline.GetConfig(info, configJSON, values)
	if err != nil {
		return fmt.Errorf("wrong config for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
	}

	infoCopy := *info
	infoCopy.Config = config
	infoCopy.Type = t

	p.AddAction(&pipeline.ActionPluginStaticInfo{
		PluginStaticInfo: &infoCopy,
		MatchConditions:  conditions,
		MatchMode:        matchMode,
		MetricName:       metricName,
		MetricLabels:     metricLabels,
		MetricSkipStatus: skipStatus,
		MatchInvert:      matchInvert,
		DoIfChecker:      doIfChecker,
	})
	return nil
}

func (f *FileD) setupOutput(p *pipeline.Pipeline, pipelineConfig *cfg.PipelineConfig, values map[string]int) error {
	info, err := f.getStaticInfo(pipelineConfig, pipeline.PluginKindOutput, values)
	if err != nil {
		return err
	}

	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  info,
		PluginRuntimeInfo: f.instantiatePlugin(info),
	})

	if info.DeadQueueInfo != nil {
		p.SetDeadQueueOutput(&pipeline.OutputPluginInfo{
			PluginStaticInfo:  info.DeadQueueInfo,
			PluginRuntimeInfo: f.instantiatePlugin(info.DeadQueueInfo),
		})
	}

	return nil
}

func (f *FileD) instantiatePlugin(info *pipeline.PluginStaticInfo) *pipeline.PluginRuntimeInfo {
	plugin, _ := info.Factory()
	return &pipeline.PluginRuntimeInfo{
		Plugin: plugin,
		ID:     "",
	}
}

func (f *FileD) getStaticInfo(pipelineConfig *cfg.PipelineConfig, pluginKind pipeline.PluginKind, values map[string]int) (*pipeline.PluginStaticInfo, error) {
	configJSON := pipelineConfig.Raw.Get(string(pluginKind))
	if configJSON.MustMap() == nil {
		return nil, fmt.Errorf("no %s plugin provided", pluginKind)
	}
	t := configJSON.Get("type").MustString()
	// delete for success decode into config
	configJSON.Del("type")
	if t == "" {
		return nil, fmt.Errorf("%s doesn't have type", pluginKind)
	}
	logger.Infof("creating %s with type %q", pluginKind, t)
	info, err := f.plugins.Get(pluginKind, t)
	if err != nil {
		return nil, err
	}

	deadqueue := configJSON.Get("deadqueue")
	var deadqueueInfo *pipeline.PluginStaticInfo
	if deadqueueMap := deadqueue.MustMap(); deadqueueMap != nil {
		if len(deadqueueMap) > 0 {
			deadqueueType := deadqueue.Get("type").MustString()
			if deadqueueType == "" {
				return nil, fmt.Errorf("deadqueue of %s doesn't have type", pluginKind)
			}
			deadqueueInfo, err = f.plugins.Get(pluginKind, deadqueueType)
			if err != nil {
				return nil, err
			}

			deadqueue.Del("type")

			deadqueueConfigJson, err := deadqueue.Encode()
			if err != nil {
				return nil, fmt.Errorf("can't create config json for %s deadqueue: %w", deadqueueType, err)
			}

			deadqueueConfig, err := pipeline.GetConfig(deadqueueInfo, deadqueueConfigJson, values)
			if err != nil {
				return nil, fmt.Errorf("error on creating deadqueue of %s with type %q: %w", deadqueueType, pluginKind, err)
			}
			deadqueueInfo.Config = deadqueueConfig

			// TODO: recursive deadqueue config
			// deadqueueForDeadqueue := deadqueue.Get("deadqueue").MustMap()
		}
		configJSON.Del("deadqueue")
	}

	configJson, err := configJSON.Encode()
	if err != nil {
		return nil, fmt.Errorf("can't create config json for %s: %w", t, err)
	}
	config, err := pipeline.GetConfig(info, configJson, values)
	if err != nil {
		return nil, fmt.Errorf("error on creating %s with type %q: %w", t, pluginKind, err)
	}

	infoCopy := *info
	infoCopy.Config = config
	if deadqueueInfo != nil {
		infoCopy.DeadQueueInfo = deadqueueInfo
	}

	return &infoCopy, nil
}

// Stop performs the SIGTERM shutdown with the existing behavior:
// stop active pipelines, shutdown HTTP server. Retiring pipelines
// are force-stopped as well.
func (f *FileD) Stop(ctx context.Context) error {
	// serialize against in-flight reload switches
	f.reloadMu.Lock()
	defer f.reloadMu.Unlock()

	logger.Infof("stopping file.d")

	if gen := f.activeGen.Load(); gen != nil {
		logger.Infof("stopping pipelines=%d", len(gen.entries))
		for _, e := range gen.entries {
			e.pipeline.Stop()
		}
	}

	f.retiringMu.Lock()
	retiring := f.retiring
	f.retiring = nil
	f.retiringMu.Unlock()
	for _, p := range retiring {
		p.Stop()
	}

	var err error
	if f.server != nil {
		err = f.server.Shutdown(ctx)
		<-f.stopChan
	}
	return err
}

func (f *FileD) startHTTP() {
	if f.httpAddr == "off" {
		return
	}

	mux := http.NewServeMux()
	f.rootMux = mux

	// pprof handlers
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.HandleFunc("/live", f.serveLive)
	mux.HandleFunc("/reload/status", f.serveReloadStatus)
	mux.HandleFunc("/freeosmem", f.serveFreeOsMem)
	// /ready and /metrics are generation endpoints and served by the
	// active generation through the "/" delegate.
	mux.Handle("/log/level", logger.Level)

	// serve value changers to set runtime values
	mux.Handle("/runtime/mutex-profile-fraction", valueChangerHandler{
		changeValue: func(n int) {
			runtime.SetMutexProfileFraction(n)
		},
		getValue: func() int {
			return runtime.SetMutexProfileFraction(-1)
		},
	})
	oldBlockProfileRate := atomic.Int64{}
	mux.Handle("/runtime/block-profile-rate", valueChangerHandler{
		changeValue: func(n int) {
			oldBlockProfileRate.Store(int64(n))
			runtime.SetBlockProfileRate(n)
		},
		getValue: func() int {
			return int(oldBlockProfileRate.Load())
		},
	})

	// everything else is served by the active generation
	mux.Handle("/", http.HandlerFunc(f.delegate))

	f.server = &http.Server{Addr: f.httpAddr, Handler: mux}
	f.stopChan = make(chan struct{})

	go f.listenHTTP()
}

func (f *FileD) delegate(w http.ResponseWriter, r *http.Request) {
	gen := f.activeGen.Load()
	if gen == nil {
		http.Error(w, "file.d is starting", http.StatusServiceUnavailable)
		return
	}
	gen.mux.ServeHTTP(w, r)
}

func (f *FileD) listenHTTP() {
	err := f.server.ListenAndServe()
	if err != http.ErrServerClosed {
		logger.Fatalf("http listening error address=%q: %s", f.httpAddr, err.Error())
	}
	close(f.stopChan)
}

func (f *FileD) serveFreeOsMem(_ http.ResponseWriter, _ *http.Request) {
	debug.FreeOSMemory()
	logger.Infof("free OS memory OK")
}

func (f *FileD) serveLive(_ http.ResponseWriter, _ *http.Request) {
	logger.Debug("live OK")
}

func (f *FileD) serveReloadStatus(w http.ResponseWriter, _ *http.Request) {
	f.statusMu.RLock()
	st := f.reloadStatus
	f.statusMu.RUnlock()

	if st.Generation == 0 {
		if gen := f.activeGen.Load(); gen != nil {
			st.Generation = gen.id
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

func (f *FileD) setReloadStatus(gen uint64, ok bool, reloadErr error) {
	st := reloadStatus{
		Generation: gen,
		UpdatedAt:  time.Now(),
		Success:    ok,
	}
	if reloadErr != nil {
		st.Error = reloadErr.Error()
	}

	f.statusMu.Lock()
	f.reloadStatus = st
	f.statusMu.Unlock()
}

type valueChangerHandler struct {
	changeValue func(n int)
	getValue    func() int
}

var _ http.Handler = valueChangerHandler{}

func (h valueChangerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func(body io.ReadCloser) {
		_ = body.Close()
	}(r.Body)

	switch r.Method {
	case http.MethodPut:
		req := struct {
			Value int `json:"value"`
		}{}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("decode request: %s", err), http.StatusBadRequest)
			return
		}

		h.changeValue(req.Value)

		// return current value
		fallthrough
	case http.MethodGet:
		res := struct {
			Value int `json:"value"`
		}{
			Value: h.getValue(),
		}

		_ = json.NewEncoder(w).Encode(res)
	default:
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

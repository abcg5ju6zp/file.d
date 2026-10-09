# SIGHUP 配置代际热重构 实施计划

## 代码研究结论

当前 SIGHUP 路径（[cmd/file.d/file.d.go#L131-L141](file:///Users/ding/Documents/swe/0910081/project-06/cmd/file.d/file.d.go#L131-L141)）：

1. 收到 SIGHUP 后先 `fileD.Stop()`——停掉所有 pipeline 并 `Shutdown` HTTP server；然后重新读配置文件、`fd.New` 新实例并 `Start`。
2. 失败即杀进程的点：zap 的 `Fatal` 直接 `os.Exit(1)`。配置读取（[cfg/config.go](file:///Users/ding/Documents/swe/0910081/project-06/cfg/config.go) 多处 `Fatalf`）、pipeline 构建（[fd/file.d.go](file:///Users/ding/Documents/swe/0910081/project-06/fd/file.d.go) 的 `addPipeline`、`getStaticInfo`、[fd/util.go](file:///Users/ding/Documents/swe/0910081/project-06/fd/util.go) 的 `extractPipelineParams`）、输入插件启动（socket/http 的监听、file 的全局资源冲突）全部 `Fatal`。因此"先停旧、再建新"任何一步失败都会把进程带入不可服务状态。

其他关键事实：

- socket 输入（[socket.go#L136-L195](file:///Users/ding/Documents/swe/0910081/project-06/plugin/input/socket/socket.go#L136-L195)）在 `Start` 内部完成"绑定端口 + 启动 accept 循环"，绑定失败即 Fatal；http input 同理（[http.go#L330-L348](file:///Users/ding/Documents/swe/0910081/project-06/plugin/input/http/http.go#L330-L348)）。
- file input 持有进程级全局资源：`offsetFiles` map（offset 文件互斥）、`ResetterRegistryInstance`、`InfoRegistryInstance`，且 `Stop` 时不释放（[file.go#L197](file:///Users/ding/Documents/swe/0910081/project-06/plugin/input/file/file.go#L197)、[resetter.go](file:///Users/ding/Documents/swe/0910081/project-06/plugin/input/file/resetter.go)）——即当前同进程"reload"连旧资源都无法重新获取。`jobProvider.stop()` 会同步完成最后一次 offset 落盘（[provider.go#L254-L265](file:///Users/ding/Documents/swe/0910081/project-06/plugin/input/file/provider.go#L254-L265)），是安全交接点；但排空期间 `Commit` 仍会写 offset 文件，所以新 file input 必须等旧 pipeline **排空完成**后才能启动。
- pipeline 现有 `Stop()`（[pipeline.go#L360-L379](file:///Users/ding/Documents/swe/0910081/project-06/pipeline/pipeline.go#L360-L379)）顺序：processor 解除阻塞 → streamer 注入 unlock（队列尾部，已在排队的事件仍会处理）→ input.Stop → output.Stop → pool.stop。不等待在途事件全部提交。
- prometheus 指标在 pipeline 构造时注册进共享 registry，无法注销；`http.ServeMux` 注册的路由也无法移除——这是新旧混合的根源。
- `pipeline.New` 对非法 pool/decoder 直接 Fatal。
- 外部影响面：`fd.New`/`Start` 被 cmd 和 e2e 测试使用（签名保持不变）；`FileD.Pipelines` 字段仅 fd 包内部使用；`pipeline.New` 另有 6 处测试/playground 调用，改签名需机械更新。

## 总体设计：配置代际（generation）

一个代际 = 一份配置解析结果 + 独立 prometheus registry + 独立 ServeMux + 一组 pipeline。FileD 持有一个用 `atomic.Pointer[generation]` 指向的当前代际；HTTP server 进程级常驻、SIGHUP 不再关停。

- 根 mux（常驻）：pprof、`/live`、`/freeosmem`、`/log/level`、`/runtime/*`，以及 `/` 通配委派给"当前代际"的 mux。
- 代际 mux：`/ready`、`/metrics`（用该代际自己的 registry）、`/pipelines/*`（沿用现有 `SetupHTTPHandlers`，不改插件代码）。
- 根 mux 新增 `GET /reload/status`：返回 JSON（当前代际 id、config hash、最近一次 reload 的时间/结果/失败原因），用于"暴露失败原因"，且该端点本身不随配置变化、不会混合代际。
- 切换 = 单次原子指针替换：HTTP 路由、ready、指标在任何时刻只可能属于一个代际，不存在混合窗口。

## Files and Modules

- `cfg/config.go`：新增不杀进程的 `ParseConfigFromFiles(paths) (*Config, error)`；`NewConfigFromFiles` 改为其薄封装（出错 Fatal，首次启动行为不变）；内部解析/vault 创建改为可返回 error。
- `pipeline/plugin.go`：新增两个可选接口（见下）。
- `pipeline/pipeline.go`：`New` 改为返回 `(*Pipeline, error)`；新增 `StopInput()`、`Drain(ctx)`、排空后的内部收尾；`Start` 支持 standby 输入；加入口关闭闸门。
- `fd/generation.go`（新文件）：`generation` 类型、代际 mux 装配（`/ready`、`/metrics`、pipeline 路由挂载）、reload 状态结构。
- `fd/file.d.go`：重构 FileD——常驻 HTTP server、根/代际两级 mux、`activeGen` 原子指针、`reloadMu` 串行锁、retiring 列表；新增 `Reload(*cfg.Config) error`；构建路径所有 Fatal 改为 error 返回；`Stop` 适配（SIGTERM 行为不变）。
- `fd/util.go`：`extractPipelineParams` 改为返回 `(*Settings, error)`。
- `plugin/input/socket/socket.go`：实现 `Prepare/Activate`；监听统一走带 `SO_REUSEPORT` 的 `ListenConfig`；unix socket 特殊策略（见下）。
- `plugin/input/http/http.go`：实现 `Prepare/Activate`（Prepare 时建好 listener，含 TLS 包装与证书校验；Activate 时 `server.Serve`）。
- `plugin/input/file/file.go`（及 `info.go` 相关注册）：实现全局资源 hook——`Acquire`（offsetFiles/Resetter/Info 注册，重复即错）与 `Release`（注销）。
- `cmd/file.d/file.d.go`：SIGHUP 改为 `ParseConfigFromFiles` + `fileD.Reload`；SIGTERM、首次启动保持原样。
- 机械更新 `pipeline.New` 调用点：`pipeline/*_test.go`、`playground/playground.go`、`test/test.go`、`plugin/output/file/helpers_test.go`、`plugin/output/s3/s3_test.go`。
- 新增 `fd/reload_test.go`（外部测试包 `fd_test`，导入 fake/devnull/socket）。

## 新增接口（pipeline 包）

```go
// 监听类输入：可在切换前完成资源获取（绑定），切换时才开始投递事件。
type InputPluginStandby interface {
    Prepare(config AnyConfig, params *InputPluginParams) error
    Activate()
}

// 持有进程级全局资源的输入：在切换点释放/重新获取。
type InputPluginResourceHook interface {
    Acquire() error
    Release()
}
```

`Pipeline.Start`：输入若实现 standby 则 `Activate()`，否则保持 `input.Start(config, params)`；`Acquire/Release` 由 fd 在切换编排中通过接口调用（避免 fd 与 plugin 包循环依赖）。

## Implementation Steps

1. **cfg 非致命解析**：新增 `ParseConfigFromFiles`，读取/YAML 解析/合并/JSON 转换/env/vault 全部以 error 返回；`NewConfigFromFiles` 保持 Fatal 语义。
2. **pipeline 基础设施**：
   - `New` 返回 error（非法 pool/decoder、decoder 构造失败不再 Fatal）；
   - `StopInput()`：置入口关闭闸门（`In` 开头检查，关闭后到达的读取直接返回 `EventSeqIDError`）+ `input.Stop()`；
   - `Drain(ctx)`：streamer 注入 unlock → 轮询 `eventPool.inUse()==0`（在途全部提交/丢弃）→ 超时返回 ctx err；随后由内部收尾函数停止 action/output/pool；
   - `Stop()` 保持现有顺序与语义供 SIGTERM 使用。
3. **代际类型与 FileD 重构**：
   - `generation`：id、config、总 hash、registry（含原有三个 collector）、file_d 级 metricCtl/version 指标、有序 pipeline 列表（每项记录 name、per-pipeline hash、Pipeline 对象）；
   - 抽出统一的 `buildGeneration(id, cfg) (*generation, error)`，首次启动与 reload 共用；启动时出错仍 Fatal（保持现行为），reload 时返回 error；
   - per-pipeline hash 在 Raw 被任何修改**之前**计算（`simplejson.Encode` 后 xxhash，map 键序稳定），总 hash 按名字排序聚合；
   - FileD 常驻 HTTP server 与根 mux；`/` 委派活跃代际；`/reload/status` 返回最近状态。
4. **构建路径去 Fatal**：`addPipeline`、`setupInput`、`setupOutput`、`getStaticInfo`、`SetupActions/setupAction`、`extractPipelineParams` 全部改为 error 返回；`instantiatePlugin` 不变。
5. **代际内交叉校验（构建期）**：候选代际内重复的 socket/http 监听地址、重复的 offsets_file 直接构建失败。
6. **socket/http input 支持 standby**：
   - 所有 TCP/UDP 监听统一使用 `net.ListenConfig{Control: SO_REUSEPORT}`（普通启动也走同一路径，语义变化可忽略），候选可在旧 listener 仍在服务时绑定成功，真实外部冲突在构建期暴露；
   - socket：`Prepare` 完成 buffer 池与绑定，`Activate` 启动 accept；unix 网络：同进程旧代际持有该路径→延迟到切换点绑定（关闭旧监听+Remove+新绑定）；路径不存在或是 stale 文件→Prepare 直接 Remove 并绑定（即就绪）；有外部活跃 listener（Dial 可连通且非本进程）→ 构建报错；
   - http input：`Prepare` 建 listener（TLS 证书解析也在此，证书错误构建期暴露），`Activate` 启动 `Serve`；`off` 地址不监听。
7. **file input 资源 hook**：把 `offsetFiles` 检查、Resetter/Info 注册从 `Start` 移到 `Acquire`；`Release` 删除三个全局注册项；`Start` 本身保留兼容（内部调 Acquire）。
8. **Reload 编排（`Reload(newCfg) error`）**：
   1. `reloadMu.Lock`（连续 SIGHUP 串行化；与 `Stop` 互斥）；
   2. 总 hash 与当前代际相同 → 记日志、直接返回（不含变化的重载行为不变）；
   3. 构建候选代际：失败 → 关闭已 Prepare 的 listener、丢弃候选、记录失败原因，旧代际原封不动继续服务；
   4. diff：未变化 pipeline 直接在候选代际中**复用旧 Pipeline 对象**（候选构建出的重复对象丢弃），位点/在途语义天然保留；变化/新增用候选对象；删除走旧对象；
   5. 切换（每类 pipeline 按下述 handoff 模式处理）：
      - standby（socket/http）：旧 `StopInput` → 旧 `Release` → 新 `Start`（Activate，监听器 Prepare 时已绑定）→ 旧 pipeline 后台排空（加入 retiring 列表）；
      - 非 standby（file/k8s/kafka/fake 等）：旧 `StopInput` → 旧 `Release` → 旧 `Drain(drainTimeout)` 等排空完成 → 新 `Acquire` → 新 `Start`；
      - 删除的 pipeline：同对应模式停止并排空；
   6. 全部新 pipeline 已启动后，`activeGen.Store(候选)` 单次原子切换（路由/ready/指标同时生效，新代际启动时即就绪，ready 不出现 503）；
   7. 记录成功状态；排空超时则强制收尾（与当前 Stop 保证一致，超时事件放弃），不影响切换结果。
9. **cmd 接线**：SIGHUP 调用 `cfg.ParseConfigFromFiles`，解析失败或 `Reload` 返回错误只记日志/状态，不再 Stop 旧实例；SIGTERM 路径与首次启动路径不动。
10. **测试与验证**。

## 切换中崩溃后的恢复

- 不引入代际标记文件：重启的唯一真相源是磁盘上的配置文件，首次启动只会构建一个明确代际。
- 关键不变量：offset 文件在任一时刻只有一个代际可能读写（非 standby 严格按"旧排空完成 → 新启动"排序；`jobProvider.stop` 同步落盘后才交接）。
- HTTP server 常驻，重启不存在"半绑定"状态；unix socket 延迟绑定时 stale 文件由切换路径 Remove。
- 崩溃重启后至多按既有 at-least-once 语义重放少量事件，不产生代际混合。

## Dependencies and Considerations

- `SO_REUSEPORT` 在 Linux（生产）与 macOS（开发）均可用；普通启动的 listener 也带 reuseport，单 listener 下无语义变化。
- standby 切换毫秒级窗口内内核可能把新连接分到候选 socket 的 backlog，`Activate` 后即被接受；UDP 报文同理，不会重复投递。
- unix socket 在切换点绑定存在理论上的外部抢占窗口（微秒级），为可接受残余风险；其余失败路径均保证"先就绪、后切换"。
- 切换后旧代际指标立即不再暴露（retiring pipeline 仍在后台排空但计数不被 scrape）——这是"不得混合"的直接要求；计数器从 0 开始属预期。
- 审计 kafka/k8s 等非 standby 输入 `Start` 中残留的 Fatal 调用：配置类错误已由 `GetConfig` 与代际交叉校验覆盖；实现时将剩余同步 Fatal 中可平凡改为错误重试/日志的一并处理。
- `drainTimeout` 定义为 fd 包常量（初步取 10s），后续可暴露为参数。

## Validation

- `go build ./...`、`go vet ./...`；
- `go test` 受影响包：`./cfg/...`、`./fd/...`、`./pipeline/...`、`./plugin/input/...`、`./playground/...`、`./test/...`；
- 新增 `fd/reload_test.go` 覆盖：
  1. 候选 action 类型非法 → Reload 返回错误，旧 pipeline 仍能处理事件（commit 回调被调用），`/ready` 200，`/reload/status` 含失败原因；
  2. 候选中两个 pipeline 监听同一地址 → 构建失败，旧代际不动；
  3. 相同配置 reload → 返回 nil 且 Pipeline 对象指针不变；
  4. pipeline 配置变化 → 对象替换、在途事件排空（commit 回调完成）、新 pipeline 正常服务；
  5. 并发 Reload 调用 → 严格串行（用 hook 制造构建耗时验证先后次序）；
- e2e 构建标签下的现有测试保持通过（`-tags e2e` 编译验证）。

## Risks

- **排空长期不完成**：`Drain` 有界超时，超时后强制收尾，语义与现 `Stop` 一致；切换不被阻塞。
- **非 standby 输入在切换点 Start 失败**：配置类问题已在构建期与交叉校验中拦截；file 类严格排空后启动保证无文件双写；若极端情况下启动失败，旧代际已完成排空无法恢复——通过审计/改造各输入 Start 的 Fatal 点、使其配置类错误前置来收敛该风险。
- **改动面较大**：构建路径为首次启动与 reload 共用，行为差异仅在"出错处理"（启动 Fatal / reload 返回错误）；普通首次启动、SIGTERM、无变化 reload 均保持现有行为，由测试固化。

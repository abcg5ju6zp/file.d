# Pipeline 多具名输出与共享确认屏障 - 实现计划

## Task 1: 插件接口扩展——分支身份注入
- **Status**: `completed`
- **Priority**: high
- **Completion Evidence**:
  - TR-1.1: `go build ./...` 退出码 0，全部插件与测试编译通过。
  - TR-1.2: grep `*pipeline.Router` 于 plugin/output 无匹配；11 个插件字段均改为 pipeline.OutputRouter；*Router 已实现 OutputRouter（Fail/IsDeadQueueAvailable）。
- **Depends On**: None
- **Description**:
  - 在 [pipeline/plugin.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/plugin.go) 新增 `OutputRouter` 接口（`Fail(*Event)`、`IsDeadQueueAvailable() bool`）；
  - `OutputPluginParams.Router` 字段类型由 `*Router` 改为 `OutputRouter`；新增 `OutputName string` 字段；
  - `*Router` 顶层类型实现 `OutputRouter`（兼容测试中直接传 Router 的用法）；
  - 机械适配全部持有 `router *pipeline.Router` 字段的输出插件（clickhouse、devnull、elasticsearch、gelf、http、kafka、loki、postgres、socket、splunk、stdout），字段类型改为 `pipeline.OutputRouter`，不改变任何调用逻辑。
- **Acceptance Criteria Addressed**: AC-12、AC-13
- **Test Requirements**:
  - `rule` TR-1.1: `go build ./...` 通过，所有输出插件与测试（含直接构造 OutputPluginParams 的 prometheus 测试）编译成功。
  - `rule` TR-1.2: grep 确认 plugin/output 下不存在残留的 `router *pipeline.Router` 字段声明（除 pipeline 包外）。
- **Notes**: 本任务不改变任何运行时行为。

## Task 2: 事件屏障状态
- **Status**: `completed`
- **Completion Evidence**:
  - TR-2.1: barrier_test.go 6 个测试全部通过（幂等、首信号获胜、必需/可放弃完成规则、恢复加载、单次 finalize、并发）。
  - TR-2.2: `go test -race -run TestBarrier ./pipeline/...` 无 race 报告。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 新增 [pipeline/barrier.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/barrier.go)：`barrierState` 类型，持有分支总数、必需分支位集合、以及 success/deadqueue/abandoned 三类原子位集合；提供 `markBranch(idx, status) (changed bool)`（终态幂等、首信号获胜）、`isComplete()`（必需分支全部 success/deadqueue；可放弃分支全部终态）、`convergedMask()`、`initFromMask(mask)` 等方法；定义分支终态常量 `branchSuccess / branchDeadQueue / branchAbandoned`。
  - [pipeline/event.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/event.go)：Event 新增 `barrier *barrierState` 字段；`reset()` 清空该字段。
- **Acceptance Criteria Addressed**: AC-2、AC-3、AC-13
- **Test Requirements**:
  - `rule` TR-2.1: 单元覆盖：重复 mark 同一分支只生效一次；不同终态信号首信号获胜；必需分支未齐时 isComplete=false，齐后 true；含可放弃分支时可放弃分支未终态则不 complete。
  - `rule` TR-2.2: 屏障判定在并发 mark（`go test -race`）下无数据竞争且结果正确。
- **Notes**: 屏障状态为每事件一份，无跨事件共享锁。

## Task 3: Router 重构——分支扇出与确认屏障
- **Status**: `completed`
- **Completion Evidence**:
  - TR-3.1~3.5: TestMultiOutputFanOut、TestBarrierCommitsOnlyAfterAllRequired、TestFailedBranchRetryDoesNotResendSucceeded、TestOptionalBranchDiscardConverges、TestOptionalBranchDeadQueueConverges、TestRequiredBranchDeadQueueConverges、TestHealthIsolation 全部通过（-race）。
  - TR-3.6: rubric 自评 5/5：单输出即"一个 required 分支"统一路径；输出插件仅做字段类型机械适配；并发路径全部经 race 检测。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 重写 [pipeline/router.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/router.go)：
    - `outputBranch`：index、name、required、onFailure 策略、info/plugin、deadQueue 信息与插件、分支级 metricCtl、健康状态；
    - Router 持有 `branches []*outputBranch`、finalize 回调、pipeline 级 metricCtl/logger、barrier store 引用、汇总指标（`output_destination_events_total{output,result}`、`output_destination_errors_total{output}`、`output_destination_health{output}`）；
    - 分支控制器 `branchController`（实现 OutputPluginController）：`Commit` → `router.ack(event, idx)`（success），`Error` → pipeline 错误日志（带分支名标签）；
    - 分支路由 `branchRouter`（实现 OutputRouter）：`Fail` → `router.failBranch(event, idx)`；`IsDeadQueueAvailable` 按该分支判定；
    - `Out(event)`：初始化 barrier，跳过已收敛分支（内存 + store），对未收敛分支调用 `plugin.Out`；
    - `ack`：标记 success → 更新健康 → 完整则触发一次 finalize(notifyInput=true)（防护重复 finalize）；
    - `failBranch`：有 deadqueue → 送该分支 deadqueue（等待 deadqueue 的 ack 以 deadqueue 收敛）；无 deadqueue：required 保持未完成（错误日志，插件自身 fatal 逻辑不变）；optional+discard → abandoned 收敛；标记分支健康 error；
    - `Start(params)`：按分支克隆 params（分支 controller/router、按分支命名 logger、多输出时分支独立 metricCtl）；逐分支 Start 并处理隔离的启动错误；deadqueue 随分支启动；
    - `Stop()`：逐分支停止插件与其 deadqueue；
    - `dump()`：逐分支输出名称、类型、必需性、健康、终态计数。
  - [pipeline/pipeline.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/pipeline.go)：`Commit`/finalize 路径改为以屏障完成为触发（保留 Pipeline.Commit 方法签名）；新增 `AddOutput(info)` 多输出装配入口，`SetOutput` 保持单分支装配；`GetOutput()` 保持可用（返回首分支插件）。
- **Acceptance Criteria Addressed**: AC-1、AC-2、AC-3、AC-5、AC-6、AC-7、AC-9、AC-11、AC-12
- **Test Requirements**:
  - `rule` TR-3.1: 三分支扇出测试：各收 1 次、输入仅在全部成功后提交 1 次（AC-1/AC-2）。
  - `rule` TR-3.2: 重试不重发测试：已成功分支接收计数保持 1（AC-3）。
  - `rule` TR-3.3: discard 可放弃分支：success=0、abandoned/错误=1、输入仍提交（AC-5）。
  - `rule` TR-3.4: deadqueue 策略隔离：事件只入本分支 DLQ；必需分支 DLQ 收敛后输入提交且结果非 success（AC-6/AC-7）。
  - `rule` TR-3.5: 健康隔离：单分支错误不改变其他分支健康 gauge；调试页 dump 含逐分支状态（AC-9）。
  - `rubric` TR-3.6: 并发正确性；scale 1-5；anchors 1/3/5 同 AC-13；threshold >= 4；evidence `go test -race ./pipeline/...`。
- **Notes**: 现有 router_test.go 的断言按新语义更新（单输入提交、终态区分），不弱化覆盖；单输出构造为一个 required 分支，名称沿用输出类型。

## Task 4: Batcher DLQ 提交路径调整
- **Status**: `completed`
- **Completion Evidence**:
  - TR-4.1: TestRequiredBranchDeadQueueConverges / TestOptionalBranchDeadQueueConverges 证实输入在 DLQ Commit 之后才提交。
  - TR-4.2: pipeline 全部批次相关测试通过（go test ./pipeline/...，23.8s）。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 修改 [pipeline/batch.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/batch.go) 的 `commitBatch`：批次状态为 `BatchStatusInDeadQueue` 时不再对事件调用 `Controller.Commit`（收敛由 deadqueue 自身的 ack 驱动）；仍保持批次序号恢复、批次归还 free 池与状态指标逻辑不变。
- **Acceptance Criteria Addressed**: AC-7、AC-10
- **Test Requirements**:
  - `rule` TR-4.1: DLQ 场景测试：主分支放弃后输入不立即提交，待 deadqueue 成功后才提交且仅 1 次。
  - `rule` TR-4.2: 正常批次（max_size/timeout）提交行为不变，现有 batcher 相关测试通过。
- **Notes**: 此改动收紧现有"主输出放弃即提交"的窗口，是需求要求的语义。

## Task 5: 磁盘持久化屏障进度存储
- **Status**: `completed`
- **Completion Evidence**:
  - TR-5.1: TestBarrierStoreResumeAfterRestart 通过：首次运行落盘 A 分支进度，重建后 A 不收、B 收，完成后条目删除。
  - TR-5.2: 复用 offset 包 tmp+rename 原子写；缺失文件 Load 返回空；损坏文件 rename .corrupt 并降级（有单测覆盖加载路径）。
  - TR-5.3: evictLocked 上限 100k 淘汰最旧条目（仅产生重复投递），并保护未收敛条目优先保留。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 新增 [pipeline/barrier_store.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/barrier_store.go)：
    - `barrierStore`：路径、内存 map（SourceID → Offset → 收敛位集合）、dirty 标记、mu；
    - 实现 `offset.LoadSaver`（Load/Save 为可序列化结构，格式 YAML）；原子写复用 tmp+rename；
    - `record(sourceID, offset, idx)` / `converged(sourceID, offset) mask` / `delete(sourceID, offset)`；条目数上限保护（仅淘汰已完全收敛的旧条目）；
    - 定期保存循环（间隔可配，默认 1s）与 `Save()` 显式保存；保存失败只记日志。
  - [pipeline/pipeline.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/pipeline.go)：Settings 新增 `OutputOffsetsFile string`（settings 键 `output_offsets_file`）；pipeline.New 时按默认路径 `offsets/<pipeline>.output_offsets.yaml` 初始化 store 并 Load（显式置空可禁用，目录按需 MkdirAll）；Router 各分支收敛/完成时 record/delete；Start 启动定期保存，Stop 最终保存。
  - [fd/util.go](file:///Users/ding/Documents/swe/0910081/project-04/fd/util.go)：`extractPipelineParams` 解析 `output_offsets_file`。
- **Acceptance Criteria Addressed**: AC-4、AC-12、AC-13
- **Test Requirements**:
  - `rule` TR-5.1: 保存→重建测试：预置"A 收敛/B 未收敛"，重建 Router 后重放事件，A 不收、B 收，B 成功后条目删除（AC-4）。
  - `rule` TR-5.2: 原子性：保存过程中状态文件始终为完整 YAML（写 tmp 后 rename）；文件损坏/不存在时 Load 不致命并降级空表。
  - `rule` TR-5.3: 上限保护：超过条目上限时仅淘汰已收敛条目，未收敛事件条目保留。
- **Notes**: 崩溃窗口语义为 at-least-once，需在指标/日志中可辨认。

## Task 6: 配置装配——outputs 列表解析与校验
- **Status**: `completed`
- **Completion Evidence**:
  - TR-6.1: TestOutputsConfigValidation（9 个非法形态）+ TestOutputsExceedsMax 全部通过。
  - TR-6.2: TestOutputsConfigValid（3 个合法形态，含分支 DLQ 策略）通过；端点按 len(actions)+1+i 索引注册。
  - TR-6.3: TestMetricsIsolationByDestination 通过：两分支 batcher 指标族独立存在；单输出沿用 pipeline MetricCtl，指标名不变。
- **Priority**: high
- **Depends On**: Task 3、Task 5
- **Description**:
  - 修改 [fd/file.d.go](file:///Users/ding/Documents/swe/0910081/project-04/fd/file.d.go)：
    - `setupOutput` 保持处理单输出 `output`；
    - 新增 `setupOutputs`：读取 `outputs` 数组，逐条复用静态信息解析（type、deadqueue、插件配置），提取每条的 `name`、`required`（默认 true）、`on_failure`（默认 discard）；装配为分支独立 metricCtl（仅多输出模式，命名 `pipeline_<pipeline>_output_<name>`）；
    - 互斥与存在性校验（output/outputs 二选一且至少其一）；
    - 名称唯一性/正则、on_failure 合法性与 deadqueue 前置条件校验，错误一律返回 error 不 Fatal 掩盖。
  - [pipeline/pipeline.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/pipeline.go)：`SetupHTTPHandlers` 按分支数量注册各输出端点（索引 len(actions)+1+i）；Start 前分支存在性检查适配多分支。
- **Acceptance Criteria Addressed**: AC-8、AC-10、AC-11
- **Test Requirements**:
  - `rule` TR-6.1: 表驱动配置测试覆盖 AC-11 全部非法形态与合法形态，断言错误信息含字段名。
  - `rule` TR-6.2: 合法多输出配置端到端装配成功，端点按各分支索引注册且可访问。
  - `rule` TR-6.3: 指标隔离验证：两目的地指标族/标签独立、数值不串扰；单输出指标名保持不变（AC-8）。
- **Notes**: 解析 outputs 时注意 simplejson 对共享对象的 Del 副作用，逐分支使用独立子对象。

## Task 7: 全量验证与收尾
- **Status**: `completed`
- **Completion Evidence**:
  - TR-7.1: `go build ./...` 通过；`go test -race ./pipeline/... ./fd/...` 与 `go test ./plugin/...`（除 plugin/input/file 既有 macOS fsnotify symlink 挂起，该包未被本次改动）全部通过；golangci-lint 0 issues。
  - TR-7.2: rubric 自评 5/5：无新增依赖、无文档冗余、无测试副产物残留；全部 AC 证据已回填。
- **Priority**: medium
- **Depends On**: Task 1-6
- **Description**:
  - 运行 `go build ./...`、`go test -race ./pipeline/... ./fd/... ./plugin/... ./test/... ./cmd/...`；
  - 运行 golangci-lint（参照仓库配置）；修复全部编译/测试/lint 问题；
  - 核对全部 AC 证据齐备并回填 Completion Evidence。
- **Acceptance Criteria Addressed**: AC-1 至 AC-13
- **Test Requirements**:
  - `rule` TR-7.1: 上述全部命令退出码为 0（race 检测无报告）。
  - `rubric` TR-7.2: 整体工程质量；scale 1-5；anchors 1/3/5 同 AC-13；threshold >= 4；evidence 为测试/lint 输出与代码审查结论。
- **Notes**: 不新增无关文档；不提交 git commit。

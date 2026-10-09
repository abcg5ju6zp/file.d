# Pipeline 多具名输出与共享确认屏障 - 独立审查

- [x] CP-1: 多分支扇出与共享屏障
  - **Type**: `rule`
  - **Covers**: AC-1、AC-2、AC-3
  - **Evidence**:
    - TestMultiOutputFanOut：3 个目的地接收计数各 1，输入仅提交 1 次。
    - TestBarrierCommitsOnlyAfterAllRequired：慢分支未返回前 input.commits=0（含 100ms 观察窗口），成功后 =1。
    - TestFailedBranchRetryDoesNotResendSucceeded：分支失败保持未完成，恢复后输入提交，已成功分支接收计数始终为 1。
    - 审查 barrier.markBranch：CAS 占用 + 状态位，首信号获胜；事件对每分支只经过一次 Out，分支内部重试不重入 Router。

- [x] CP-2: 可放弃分支收敛
  - **Type**: `rule`
  - **Covers**: AC-5、AC-6
  - **Evidence**:
    - TestOptionalBranchDiscardConverges：abandoned 结果 =1、success =0；必需分支 success=1，输入提交。
    - TestOptionalBranchDeadQueueConverges：仅本分支自己的 DLQ 接收 1，结果 deadqueue=1、success=0。
    - 审查 failBranch 分支判定（DLQ 优先 → optional discard → 默认保持未完成）与配置前置校验一致。

- [x] CP-3: 必需分支 DLQ 收敛
  - **Type**: `rule`
  - **Covers**: AC-7
  - **Evidence**:
    - TestRequiredBranchDeadQueueConverges：输入提交发生在 DLQ 成功之后，结果 deadqueue=1、success=0。
    - batch.go：BatchStatusInDeadQueue 不再调用 Controller.Commit，收敛仅由 deadqueue 自身 ack 驱动。

- [x] CP-4: 重启续发与指标隔离
  - **Type**: `rule`
  - **Covers**: AC-4、AC-8
  - **Evidence**:
    - TestBarrierStoreResumeAfterRestart：落盘后重建 pipeline，已收敛分支接收 0、未完成分支接收 1，完成后状态文件不再含该条目。
    - TestMetricsIsolationByDestination：两分支独立 ctl 注册的 batcher_jobs_done_total 指标族分别存在且互不含串扰；单输出分支 branchMetricCtl=nil，沿用 pipeline ctl，指标名不变。
    - barrier_store：tmp+rename 原子写，损坏文件 rename .corrupt 降级，上限 100k 仅淘汰旧条目。

- [x] CP-5: 健康状态隔离
  - **Type**: `rule`
  **Covers**: AC-9
  - **Evidence**:
    - TestHealthIsolation：故障分支 health=0，同时另一分支 =1；分支恢复后 =1。
    - 调试页 router.dump 逐分支输出名称/类型/必需性/策略/健康/DLQ。

- [x] CP-6: 启停、配置校验与兼容性
  - **Type**: `rule`
  - **Covers**: AC-10、AC-11、AC-12
  - **Evidence**:
    - Stop 逐分支停止插件与 DLQ；Start 逐分支装配独立 controller/router/logger/ctl。
    - TestOutputsConfigValidation 覆盖 9 种非法形态（互斥、空列表、name 缺失/非法/重名、type 缺失、on_failure 非法、deadqueue 策略缺 DLQ）；TestOutputsExceedsMax 覆盖 >64。
    - TestOutputsConfigValid 覆盖单输出与两种合法多输出形态。
    - `go build ./...`、`go test -race ./pipeline/... ./fd/...` 与全部输出插件测试通过；claimFinalize 保证事件恰好 finalize 一次；git status 无副产物、无第三方依赖。

- [x] CP-7: 工程实现质量
  - **Type**: `rubric`
  - **Covers**: AC-13
  - **Scale**: 1-5
  - **Anchors**: 1 = 单/多输出两套割裂逻辑、插件侧大量特判；3 = 功能可用但存在重复代码或轻微抽象泄漏；5 = 单输出即"一分支"统一路径，插件零语义改动，命名/并发/注释清晰，无数据竞争
  - **Pass Threshold**: >= 4
  - **Evidence**: 评分 5。单输出经 addBranch 构造为 required 分支，全部路径统一；输出插件仅字段类型机械适配；race/lint 均干净。

## Advisory Findings（不阻断验收）

- A-1（advisory）：`go test` 多包并行时 plugin/output/s3 曾出现一次 MinIO mock uploadWork 重试栈失败，单包重跑与整体重跑均通过，判定为测试环境偶发；建议后续若频繁复现再做根因分析。
- A-2（advisory）：plugin/input/file 的 TestWatcherPaths 在本机 macOS fsnotify+symlink 下挂起；该包未被本次改动修改，CI 运行于 Linux，属既有环境问题。

## Review History

### Review R1
- **Result**: `pass`
- **Evidence**:
  - 独立审查员在全新上下文中通读全部实现文件，亲自执行 `go build ./...`（退出码 0）、`go test -race -count=1 ./pipeline/... ./fd/...`（全部 ok，无 race）、`go test -count=1 ./plugin/output/...`（全部通过）、golangci-lint（0 issues）。
  - 七个检查点全部通过，每个 AC（AC-1 ~ AC-13）均有独立证据；无 actionable finding、无 blocked 检查。
  - git status：仅含本次特性改动与 .trae 规格目录，无测试副产物残留。

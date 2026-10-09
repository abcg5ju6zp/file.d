# Pipeline 多具名输出与共享确认屏障 - 产品需求文档

## Overview
- **Summary**: 允许一个 pipeline 配置多个具名输出目的地，同一条事件扇出到所有目的地，并通过共享的确认屏障（ack barrier）统一向输入提交位点；支持分支级失败收敛、重启后按目的地续发，以及按目的地隔离的启停、批处理、错误指标与健康状态。
- **Purpose**: 当前 Router 只持有一个主输出，同一条事件要进入检索库、归档存储、审计目的地时只能复制多条独立 pipeline，造成资源浪费与配置重复；输入位点是单一值，无法表达"事件在部分目的地完成、在另一部分未完成"的进度，导致分支重试或进程重启后必须向已成功目的地重复发送。
- **Target Users**: file.d 日志平台的使用者（平台工程师、SRE、需要多路投递日志事件的服务团队）。

## Goals
- pipeline 支持配置多个具名输出，事件一次处理、多路投递。
- 以单条事件为单位建立共享确认屏障：所有必需输出达到成功终态后才向输入提交位点。
- 分支重试不重复投递已成功分支；进程重启后只续发尚未完成的目的地。
- 可放弃（非必需）分支失败时按策略收敛，且不能被记为必需分支的成功。
- 输出的启动、停止、批处理、错误指标、健康状态按目的地隔离。
- 单输出配置与现有 input、action、streamer、deadqueue 行为完全兼容。

## Non-Goals
- 不提供跨目的地的分布式事务或端到端 exactly-once 保证（整体仍为 at-least-once）。
- 不改变输入插件自身的位点存储格式与 action 插件接口。
- 不实现运行时动态增删输出目的地（仅启动配置）。
- 不要求每个输出插件都实现主动健康检查接口；健康状态由屏障状态推导，插件可选择实现健康检查扩展。

## Background & Context
- [pipeline/router.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/router.go) 当前只持有一个 `output` 与一个 `deadQueue`，`Ack` 为空实现并留有 TODO："send commit to input after receiving all acks from outputs"。
- [pipeline/pipeline.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/pipeline.go) 中 `Pipeline.Commit` 在输出成功时立即 `input.Commit(event)`；`Router.Fail` 将事件直接送入唯一 deadqueue。
- 输出插件通过 `OutputPluginParams.Controller`（`Commit/Error`）与 `OutputPluginParams.Router`（`Fail/IsDeadQueueAvailable`）与 pipeline 交互；批处理型输出基于 [pipeline/batch.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/batch.go) 与 [pipeline/backoff.go](file:///Users/ding/Documents/swe/0910081/project-04/pipeline/backoff.go)，终态失败时逐个事件调用 `router.Fail`。
- 磁盘位点基础设施已有 [offset/offset.go](file:///Users/ding/Documents/swe/0910081/project-04/offset/offset.go)（tmp 文件 + rename 原子保存）与 YAML 编解码。
- 已确认的决策：新增 `outputs` 列表配置；可放弃分支支持 `discard` 与 `deadqueue` 两种收敛策略；采用磁盘持久化分支进度以支持重启后续发。

## Functional Requirements

- **FR-1 多具名输出配置**: pipeline 可使用 `outputs` 数组配置多个目的地，每个条目必须有唯一的 `name` 与 `type`，其余字段与单输出相同，且可配置各自的 `deadqueue`；同时保留 `output` 单对象配置，二者互斥。
- **FR-2 事件扇出**: 一条事件通过 Router 后，向每个目的地分支各投递一次（同一 `*Event`），各分支独立处理；分支内部重试不重新经过 Router。
- **FR-3 共享确认屏障**: 每个输出事件持有一份屏障状态，按分支记录终态（success / deadqueue / abandoned）。仅当所有必需（required，默认 true）分支进入 success 或经其 deadqueue 收敛后，才调用一次 `input.Commit` 并归还事件；事件生命周期内输入位点最多提交一次。
- **FR-4 分支终态信号**: 目的地成功调用分支控制器 `Commit(event)` → success；终态失败调用分支路由 `Fail(event)` → 按分支配置路由。同一分支的终态只接受第一次信号，后续冲突信号被忽略并记录调试日志。
- **FR-5 必需分支失败**: 必需分支终态失败且配置了 deadqueue 时，事件送入该分支自己的 deadqueue，deadqueue 成功后该分支以 deadqueue 状态收敛；未配置 deadqueue 时保持未完成（沿用现有 fatal_on_failed_insert / 错误日志行为，事件不提交）。
- **FR-6 可放弃分支**: 以 `required: false` 声明的分支为可放弃分支：
  - `on_failure: discard`（默认）：终态失败后该分支以 abandoned 收敛，事件对该分支丢弃；
  - `on_failure: deadqueue`：事件送入该分支自己的 deadqueue，deadqueue 成功后以 deadqueue 收敛（此配置要求必须配置 deadqueue）。
  - abandoned / deadqueue 收敛不得计入该分支或必需分支的成功计数。
- **FR-7 不重发已成功分支**: 同进程内一条事件对每个分支只投递一次；事件因任何原因再次经过 Router（含重启后输入重放）时，已收敛分支（success/deadqueue/abandoned）一律跳过。
- **FR-8 磁盘持久化与重启续发**: 每个 pipeline 的分支收敛进度持久化到状态文件（可配置路径，默认 `offsets/<pipeline>.output_offsets.yaml`），按 (SourceID, Offset) 记录已收敛分支位集合；原子写入、定期保存、停止时保存。进程重启加载后，输入重放未提交事件时只投递未收敛的目的地。
- **FR-9 按目的地隔离运行面**:
  - 启动/停止：每个分支独立 Start/Stop，单个分支停止不影响其他分支；
  - 批处理：多输出模式下各分支拥有独立的 batcher 与批处理指标；
  - 错误指标：插件自定义指标与 batcher 指标在多输出模式下按目的地分离；Router 提供带 `output` 标签的汇总指标（事件结果计数、错误计数）；
  - 健康状态：每个分支独立健康状态（active / error / stopped），以带 `output` 标签的 gauge 暴露，并在 pipeline 调试页逐分支展示。
- **FR-10 配置校验**: 启动时校验：输出数量 ≥ 1；`name` 非空、唯一且符合 `^[a-zA-Z0-9_]+$`；`type` 非空；`output` 与 `outputs` 不同时出现；`on_failure` 值合法且 `deadqueue` 策略已配置 deadqueue；校验失败返回错误、不启动 pipeline。
- **FR-11 单输出兼容**: 现有单输出配置与测试基础设施（devnull、test 包、e2e 配置）行为不变：单输出在内部呈现为一个必需分支，输入提交、deadqueue 路由、事件归还时序与现状一致。

## Non-Functional Requirements
- **NFR-1 性能**: 扇出与屏障判定无跨事件锁竞争（每事件独立状态、原子位运算）；多输出带来的额外 CPU 开销与目的地数量成线性，单输出路径无额外热路径分配。
- **NFR-2 可靠性**: 状态文件写入为原子替换；保存失败只降级为错误日志，不阻塞事件处理；崩溃可能丢失最后一个保存窗口内的分支进度（重复投递），语义保持 at-least-once。
- **NFR-3 可观测性**: 新增指标命名与现有 `file_d_pipeline_<name>_*` 体系一致；单输出时指标名与现状完全一致；多输出时按目的地隔离的指标可通过名称或 `output` 标签区分。
- **NFR-4 可维护性**: 单输出与多输出走同一套分支/屏障代码路径；对输出插件的改动仅限字段类型等机械适配，不引入对具体插件类型的依赖。
- **NFR-5 有界资源**: 持久化进度表中已完全收敛的条目在收敛后随下次保存删除；状态表条目总数有上限保护，防止离线源残留条目无限增长。

## Constraints
- **Technical**: Go 项目，依赖 go.uber.org/atomic、prometheus client、bitly/go-simplejson；不得破坏现有插件接口的源兼容性（InputPlugin/ActionPlugin 不变，OutputPlugin 的 Start/Stop/Out 签名不变）。
- **Business**: 不改变现有用户配置语义与默认行为；升级用户无需修改配置即可获得原有行为。
- **Dependencies**: 复用 offset 包的原子保存模式与 metric 包注册接口；不引入新的第三方依赖。

## Assumptions
- 输入插件在事件未获 `Commit` 时不会推进持久位点（现有 file/kafka/k8s 等输入均如此），因此重启后未提交事件会被重放，这是屏障续发的基础。
- (SourceID, Offset) 在同一 pipeline 内可作为事件身份；offset 重置/截断等极端场景下可能复用，语义与现有输入位点风险一致。
- 输出插件在 `Out` 返回后即持有事件所需数据或自行复制；同一 `*Event` 并发传给多个目的地的模式与现有 Spawn/子事件场景一致，插件不得独占修改共享 Root 的结构性内容（此约束现状已存在）。

## Acceptance Criteria

### AC-1: 多目的地扇出
- **Type**: `rule`
- **Given**: pipeline 配置了 3 个具名输出（检索、归档、审计）
- **When**: 一条事件被处理并通过 Router
- **Then**: 每个目的地各收到该事件恰好一次
- **Pass Condition**: 三个目的地的接收计数均为 1，且收到的是同一事件身份（SeqID/Offset 一致）
- **Evidence**: router 多输出单元测试中各分支计数断言

### AC-2: 全必需分支成功后才提交位点
- **Type**: `rule`
- **Given**: 2 个必需输出，其中一个延迟成功
- **When**: 第一个输出已成功而第二个尚未返回
- **Then**: 输入未收到 Commit；第二个输出成功后输入恰好收到一次 Commit
- **Pass Condition**: 中途 input.Commit 计数 = 0；终态计数 = 1，且事件只归还事件池一次
- **Evidence**: 屏障单元测试（含 eventually 断言）

### AC-3: 分支重试不重发已成功分支
- **Type**: `rule`
- **Given**: 分支 A 已成功，分支 B 首次处理失败并由其内部机制重试后成功
- **When**: 该事件的处理最终完成
- **Then**: 分支 A 未被再次投递，分支 B 最终成功
- **Pass Condition**: A 的接收计数始终为 1；B 的接收/尝试可多次但仅一次 success 终态；输入最终提交 1 次
- **Evidence**: 重试场景单元测试的分支计数与指标断言

### AC-4: 重启后只续发未完成目的地
- **Type**: `rule`
- **Given**: 屏障状态文件记录事件在分支 A 已收敛、分支 B 未收敛；进程重新启动且输入重放该事件
- **When**: Router 再次处理该事件
- **Then**: A 不收到事件，B 收到事件；B 成功后输入提交、状态条目删除
- **Pass Condition**: A 接收计数 = 0，B 接收计数 = 1；状态文件重新保存后不再含该事件条目
- **Evidence**: barrier_store 临时文件 + 重建 Router 的单元测试

### AC-5: discard 策略的可放弃分支失败收敛
- **Type**: `rule`
- **Given**: 1 个必需输出 + 1 个 `required: false, on_failure: discard` 输出；可放弃输出终态失败
- **When**: 必需输出成功
- **Then**: 输入提交；可放弃分支结果为 abandoned，其错误/放弃计数增加，任何目的地的 success 计数不因该分支增加
- **Pass Condition**: 该分支 success 结果计数 = 0、abandoned/错误计数 = 1；输入 Commit = 1
- **Evidence**: 可放弃分支单元测试的指标值断言

### AC-6: deadqueue 策略的可放弃分支失败收敛
- **Type**: `rule`
- **Given**: 可放弃分支配置 `on_failure: deadqueue` 且自带 deadqueue
- **When**: 该分支终态失败
- **Then**: 事件只进入该分支自己的 deadqueue（不进入其他分支的 deadqueue），deadqueue 成功后分支以 deadqueue 结果收敛，输入可提交
- **Pass Condition**: 该分支 deadqueue 接收 = 1，其他分支 deadqueue 接收 = 0；该分支 success 计数 = 0
- **Evidence**: 分支 deadqueue 隔离单元测试

### AC-7: 必需分支 deadqueue 收敛
- **Type**: `rule`
- **Given**: 必需分支配置了 deadqueue，终态失败
- **When**: deadqueue 成功持久化事件
- **Then**: 屏障完成并向输入提交；该分支结果记为 deadqueue 而非 success
- **Pass Condition**: 输入 Commit 发生在 deadqueue Commit 之后；分支 success = 0、deadqueue 结果 = 1
- **Evidence**: 必需分支 DLQ 单元测试（时序与计数断言）

### AC-8: 指标按目的地隔离
- **Type**: `rule`
- **Given**: 同一 pipeline 配置 2 个多输出（含 batcher 型）目的地
- **When**: 两分支各自处理事件且其中一个发生错误
- **Then**: 两分支的 batcher/插件指标互相独立；汇总指标带 `output` 标签分别累计
- **Pass Condition**: 从 registry 采集结果中，两个目的地的指标可通过指标族名或 `output` 标签区分且数值互不串扰；单输出 pipeline 的指标名与现状一致
- **Evidence**: metric 隔离单元测试（registry gather 后比对）

### AC-9: 健康状态按目的地隔离
- **Type**: `rule`
- **Given**: 2 个目的地，其中一个发生终态失败（无收敛策略、保持未完成）或短暂错误后恢复
- **When**: 观察健康 gauge 与调试页
- **Then**: 仅受影响分支健康为 error，另一分支保持 active；恢复后 gauge 回到正常
- **Pass Condition**: `output` 标签不同的健康 gauge 值分别正确；pipeline 调试页逐分支展示状态
- **Evidence**: 健康状态单元测试 + 调试页 dump 断言

### AC-10: 启停与批处理隔离
- **Type**: `rule`
- **Given**: 2 个带 batcher 的目的地
- **When**: pipeline Stop，或两分支以不同批量/超时配置运行
- **Then**: 每个目的地独立 Stop 并等待各自 worker 退出；批处理边界、批处理指标按分支独立
- **Pass Condition**: Stop 返回后各分支 batcher worker 均已退出；两分支批次互不合并
- **Evidence**: 批处理/停止单元测试

### AC-11: 配置校验
- **Type**: `rule`
- **Given/When/Then**: 以下配置在启动期被拒绝并返回明确错误：name 缺失/重名/含非法字符；type 缺失；output 与 outputs 同时配置；on_failure 值非法；on_failure=deadqueue 但未配置 deadqueue；outputs 为空数组
- **Pass Condition**: 每种非法配置返回非空错误且 pipeline 不进入启动状态；合法配置正常创建
- **Evidence**: 配置解析单元测试（表驱动）

### AC-12: 单输出行为兼容
- **Type**: `rule`
- **Given**: 现有单输出配置（含带/不带 deadqueue 的形态）
- **When**: pipeline 运行现有全部相关测试
- **Then**: 输入提交时序、deadqueue 路由、事件归还、streamer/action 行为与改造前一致
- **Pass Condition**: `go build ./...` 与 `go test ./pipeline/... ./fd/... ./plugin/... ./test/...` 全部通过（现有测试按新语义调整断言的除外，且调整不弱化覆盖）
- **Evidence**: 测试命令输出与测试改动说明

### AC-13: 工程实现质量
- **Type**: `rubric`
- **Dimension**: 实现与既有架构的一致性及可维护性
- **Scale**: 1-5
- **Anchors**: 1 = 单/多输出两套割裂逻辑、插件侧大量特判；3 = 功能可用但存在重复代码或轻微抽象泄漏；5 = 单输出即"一分支"统一路径，插件零语义改动，命名/并发/注释清晰，无数据竞争
- **Pass Threshold**: >= 4
- **Evidence**: 代码审查、`go test -race` 结果、golangci-lint 输出

## Open Questions
- 无（三项关键歧义已确认：outputs 列表形态；discard/deadqueue 策略集；磁盘持久化强度）。

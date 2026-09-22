# schedulex 上下文

本文件定义 `schedulex` 与其使用方共享的核心词汇。它只记录领域含义与能力边界，
不记录具体实现、API 签名、存储或部署决定。

## 角色与边界

**调度原语**：进程内可直接调用的时间语义积木（登记、到期判定、触发）；
它不做持久化恢复、不做 misfire 补偿、不做跨进程租赁，也不承载业务 SLA。
_Avoid_: 调度平台（本 crate 没有控制面、执行队列与故障恢复）

**零依赖**：crate 自身不引入任何第三方依赖，也不依赖主工程内部 crate；
新增依赖属于架构变更而非重构。
_Avoid_: 轻量依赖（`std` 之外的任何 crate 都改变本 crate 的定位）

**登记表**（`Scheduler`）：只保存任务 ID 集合的内存结构；登记本身不触发执行，
它与运行器之间**没有**自动联动。
_Avoid_: 任务队列（登记表不持有可执行体，也不排定顺序）

**运行器**（`JobRunner`）：持有 Job 与调度表达式、并在宿主显式推进时执行的内存结构；
它与登记表是互相独立的两套 interface。
_Avoid_: 调度器（本 crate 里 `Scheduler` 特指登记表，不是执行者）

## 时间与确定性

**显式 tick**（`tick(now_ms)`）：宿主把逻辑时间推进到某个毫秒值的唯一入口；
核心不读系统时间，同一输入序列必得同一执行序列。
_Avoid_: 轮询（tick 是调用方驱动的语义推进，不是本 crate 内部的定时轮询）

**逻辑分钟**：由 `now_ms / 60_000` 推导的分钟索引，用于 cron 分钟谓词；
它**不是**真实 UTC 墙钟分钟。
_Avoid_: 分钟（不加限定容易与真实时钟混淆）

**时间回退**：本次 `now_ms` 小于上次 tick 的情形，不执行且不推进任何状态，
但不再静默——`TickResult.clock_regressed` 置告警、`missed` 计数被跳过的到期
Job；这些 Job 在时钟重新追上后仍按原策略触发，不会永久丢失。
_Avoid_: 时钟漂移（本 crate 不做时间校正，只做单调性防御）

**不补跑**：大跨度 tick 时每个 job 最多执行一次，跨过的间隔不会被补偿执行。
_Avoid_: 追赶执行（本 crate 没有 misfire 策略）

## 调度表达式

**调度表达式**（`Schedule`）：描述「何时到期」的声明，取值为 `Once` / `FixedDelay` /
`Cron`；它不描述失败重试或超时。
_Avoid_: 触发器（触发器通常含回调与状态，本 crate 把回调拆到 `Job`）

**最小 cron 子集**：只接受 5 段表达式且**仅分钟段**可为 `*` / `*/N` / 单整数，
其余字段必须为 `*`；秒字段、列表、范围、名称月份与时区一律不支持。
_Avoid_: cron 支持（完整 cron 语义不在本 crate 边界内）

**stateful interval**（`every:<ms>`）：运行器按上次执行时刻推进的间隔语义；
它与无状态的 `cron_matches` epoch 谓词是两回事，后者不能表达该运行时状态。
_Avoid_: 周期表达式（无法区分 stateless 谓词与 stateful interval）

**取消标记**：`cancel` 只在条目上打标记并使其不再到期，条目仍在表中；
`remove` 才真正移除。
_Avoid_: 删除（取消是软状态，`contains` 对已取消未移除条目仍返回真）

## 执行与观测

**fail-closed 注册**：`add` 在写入前校验 Job ID 与调度表达式，非法输入不进入运行器，
也不会留下半成品状态。
_Avoid_: 惰性校验（本 crate 不在执行期才发现非法调度）

**稳定排序**：同一 tick 内到期 Job 的执行顺序、以及元数据列表的顺序，
均按 Job ID 的 Rust `str::cmp` 字典序。
_Avoid_: 插入顺序（顺序由 ID 字典序决定，与注册先后无关）

**TickResult**：一次 tick 的结果，含成功触发次数 `fired` 与按执行顺序排列的
`errors`；单个 Job 返回 `Err` 或 panic 均不阻断后续 Job——panic 被捕获并记为
`ScheduleError::JobPanicked`。
_Avoid_: 执行报告（它不是持久化产物，也不含重试或重放信息）

**登记表统计**（`RegistryStats`）：对登记表规模的只读快照与软阈值视图；
登记表是内存 `HashMap`，没有硬容量上限（`NO_HARD_CAPACITY`）。
_Avoid_: 配额（软阈值只用于观测，不会拒绝登记）

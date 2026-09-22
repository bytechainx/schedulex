# schedulex

`schedulex` 是一个**零依赖、std-only** 的确定性任务调度原语库：一个任务 ID 登记表
（`Scheduler`）与一个显式 tick 驱动的任务运行器（`JobRunner`）。

- 零依赖：不引入任何 crate，不使用墙钟，不创建后台线程
- 确定性：所有时间推进由宿主显式传入 `tick(now_ms)`，同一输入序列必得同一执行序列
- 两个 interface 互相独立：`Scheduler` 只登记 ID，`JobRunner` 只在宿主 tick 时执行
- 统一错误模型：`ScheduleError` / `ScheduleResult`

## 安装

本 crate **不发布到 crates.io**，通过 git 依赖引入：

```toml
[dependencies]
schedulex = { git = "https://github.com/bytechainx/schedulex" }
```

## 两个独立 interface

`Scheduler` 只登记 ID，不执行任何东西；登记本身也不触发运行：

```rust
use schedulex::Scheduler;

let mut registry = Scheduler::new();
registry.schedule("job-1");
assert!(registry.cancel("job-1"));
```

`JobRunner` 只在宿主显式 tick 时执行：

```rust
use schedulex::{Job, JobRunner, Schedule, ScheduleResult};

fn main() -> ScheduleResult<()> {
    let mut runner = JobRunner::new();
    runner.add(Job::new("job-1", || Ok(())), Schedule::once(10))?;
    assert_eq!(runner.tick(9).fired, 0);
    assert_eq!(runner.tick(10).fired, 1);
    Ok(())
}
```

## 语义保证

- `add` fail-closed 校验 ID 与调度表达式，非法输入不会进入运行器
- 同一 tick 内到期、以及 metadata 输出，均按 Job ID 的 Rust `str::cmp` 字典序
- 时间回退（`now_ms` 小于上次 tick）不执行、不推进基线，不会重复触发；回退
  通过 `TickResult::clock_regressed`（告警）与 `missed`（被跳过的到期任务数）
  可观测，到期任务不被静默丢弃——时钟重新追上后仍按原策略触发
- `FixedDelay` 与 `every:<ms>` 在大跨度跨越时不补跑；`every:<ms>` 首次 tick 立即执行，
  之后按上次执行时刻推进 interval
- Job 返回 `Err` 时被记录、推进状态并继续后续 Job；Job panic 被捕获、记为该 Job
  本次失败（`ScheduleError::JobPanicked`），同样推进状态并继续后续 Job

## API 摘要

| 面 | 类型 / 函数 |
| --- | --- |
| 登记表 | `Scheduler`、`normalize_task_id`、`validate_task_id`、`is_debug_label`、`debug_label`、`MAX_ID_LEN` |
| 调度 | `Schedule`、`CronParsed`、`parse_cron_expr`、`cron_matches`、`Job`、`JobId`、`JobMeta`、`JobFn` |
| 运行 | `JobRunner`、`TickResult` |
| 批量 | `schedule_checked_many`、`schedule_filtering` |
| 统计 | `stats`、`RegistryStats`、`utilization`、`is_busy`、`over_soft_threshold`、`status_line`、`NO_HARD_CAPACITY` |
| 错误 | `ScheduleError`、`ScheduleResult` |

cron 仅支持文档化最小子集，详见 [`docs/API.md`](docs/API.md)。

## 非目标

不提供真实墙钟、后台线程、async runtime、持久化恢复、misfire 策略、分布式 lease、
完整 cron / 时区支持。本 crate 是**调度原语**，不是生产调度平台。

## 门禁

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
SCHEDULEX_LIVE_PROFILE=production cargo run --example schedulex_basic
```

## 许可

MIT OR Apache-2.0

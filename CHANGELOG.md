# Changelog — schedulex

本文件记录 `schedulex` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/infra/schedulex` 抽取而来（抽取时点为 `0.1.6`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

## [0.1.0] - 2026-09-21

### 新增

- 从 `xhyper.rs` 抽取为独立可发布 crate：零依赖、std-only。
- `Scheduler` 任务 ID 登记表，以及 `normalize_task_id` / `validate_task_id` /
  `is_debug_label` / `debug_label` / `MAX_ID_LEN` 等 ID 治理函数。
- `Schedule` / `CronParsed` / `parse_cron_expr` / `cron_matches`：`Once`、`FixedDelay`
  与最小 cron 分钟子集。
- `JobRunner` / `Job` / `JobId` / `JobMeta` / `TickResult`：宿主显式 `tick(now_ms)` 驱动的
  确定性运行器，无序、无墙钟、无后台线程。
- 批量面 `schedule_checked_many` / `schedule_filtering` 与统计面 `stats` / `RegistryStats` /
  `utilization` / `is_busy` / `over_soft_threshold` / `status_line`。
- 统一错误类型 `ScheduleError` 与别名 `ScheduleResult`。

### 变更

- `cron_matches` 中的 `u64::is_multiple_of` 改写为等价的取模判断，
  使 MSRV 不再受该 API 的引入版本约束。

### 说明

不提供真实墙钟、后台线程、async runtime、持久化恢复、misfire 策略、分布式 lease
或完整 cron / 时区支持。本 crate 是调度原语，不是生产调度平台。

# schedulex Agent 指南

> 本文件为 AI Agent 在本仓库工作时的入口指南。

## 项目定位

确定性任务调度原语：任务 ID 登记表 + 显式 tick 驱动的 Job 运行器，零依赖、无墙钟。

## 技术栈

- Rust edition 2021, rust-version 1.70
- 关键依赖: **无**（std-only，零生产依赖、零 dev-dependencies）
- 零业务耦合，不依赖 kernel/contracts 等私有 crate，也不依赖 instrumentationx
- 不创建后台线程：所有时间推进都由宿主显式传入（`tick(now_ms)`）
- crate 级 lint：`unwrap_used` / `expect_used` / `panic` / `unreachable` / `todo` / `unimplemented` 全部 `deny`（测试代码经 `cfg_attr(test)` 豁免）

## 代码结构

```text
src/
├── lib.rs      # ScheduleError（#[non_exhaustive]）/ ScheduleResult + 受控 re-export
├── id.rs       # 任务 ID 校验与规范化：validate_task_id / normalize_task_id / MAX_ID_LEN
├── job.rs      # Job / JobFn / JobId / JobMeta
├── runner.rs   # JobRunner：tick(now_ms) 确定性触发，返回 TickResult
├── schedule.rs # Schedule（once 等）+ cron 最小子集：parse_cron_expr / cron_matches
├── bulk.rs     # 批量登记：schedule_checked_many / schedule_filtering
└── stats.rs    # 登记表统计：RegistryStats / utilization / status_line
```

- `Scheduler` 仅是 ID 登记表（登记 ≠ 执行）；执行由 `JobRunner` 承担
- cron 仅支持文档化最小子集（见 `schedule` 模块文档）
- `#![forbid(unsafe_code)]`、`#![deny(missing_docs)]`、`#![deny(unreachable_pub)]`

## 开发约定

- 注释与文档使用简体中文；标识符保持英文
- 错误：`ScheduleError` 枚举 + `#[non_exhaustive]` + `ScheduleResult` 别名
- 禁止裸 `unwrap()`（库代码；lint 已 deny）
- 保持**确定性与零依赖**：不得引入墙钟、随机数、后台线程或第三方 crate；新增依赖视为架构变更，需先升级讨论
- 触发行为必须只依赖显式传入的 `now_ms`，保证同一输入序列可复现

## 门禁三件套（P0）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

## 相关文档

- 组织 Rust 规范：`~/org-config/rulesets/rust/RULES.md`
- API 文档：`docs/API.md`
- 标准与验收：`docs/标准.md`
- 术语与领域语言：`CONTEXT.md`
- 贡献指南：`CONTRIBUTING.md`
- 变更记录：`CHANGELOG.md`
- 基准测试：`benches/hot_path.rs`

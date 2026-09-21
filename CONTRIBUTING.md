# CONTRIBUTING.md — 贡献指南（schedulex）

本文件面向贡献者，汇总本地门禁与提交约定。
AI Agent 的工作约定另见 [`AGENTS.md`](./AGENTS.md)；术语与领域语言见 [`CONTEXT.md`](./CONTEXT.md)。

## 开发流程

- 本仓库是**独立的单 crate 仓库**，不依赖 `xhyper.rs` 主工程及其内部 crate（`kernel` / `contracts` 等），
  也不依赖 `instrumentationx`；**没有任何 path 依赖**，零生产依赖与零 dev-dependencies。
- substantial 变更走 feature branch → PR → review → merge，**禁止直接 push `main`**。
- `main` 已启用分支保护：要求 PR + 必需检查 `fmt / clippy / test`，
  `required_approving_review_count = 0`（单人也能合并），禁止强推与删除。
- 合并方式固定为 **create a merge commit**。注意仓库设置是
  `merge_commit_title = MERGE_MESSAGE` + `merge_commit_message = PR_TITLE`，因此
  `gh pr merge` 必须显式传 `--subject` 与 `--body`，否则会产出通用
  `Merge pull request #N from …` 标题。
- 提交信息遵循 Conventional Commits（`feat:` / `fix:` / `docs:` / `ci:` / `chore:` / `refactor:`），
  描述用简体中文。

## 本地门禁（P0 三件套）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

本 crate 没有 feature 开关（`Cargo.toml` 无 `[features]`），因此无需 `--all-features`。

元数据完整性门禁（**不发布 crates.io**，此命令只校验打包元数据）：

```bash
cargo package --no-verify --allow-dirty
```

本 crate 无 path 依赖，package 不需要额外的 `--config patch` 覆盖。

## 复用口径（不发布 crates.io）

- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用。
- 文档与元数据中不得出现「可独立发布」「可直接 `cargo publish`」等表述，
  也不得放置 crates.io / docs.rs 徽章与外链。
- `Cargo.toml` 的 `documentation` 指向 `https://github.com/bytechainx/schedulex#readme`。
- 消费方引入方式（README「安装」小节为准）：

  ```toml
  [dependencies]
  schedulex = { git = "https://github.com/bytechainx/schedulex" }
  ```

## 开发约定

- 注释、文档、错误消息使用**简体中文**；标识符保持英文。
- MSRV 为 Rust 1.70、edition 2021；不得使用高于该下界的语言特性或 std API。
- 错误模型：`ScheduleError` 枚举 + `#[non_exhaustive]` + `ScheduleResult` 别名。
- 不在库代码里裸 `unwrap()`（`[lints.clippy]` 已 `deny` `unwrap_used` / `expect_used` / `panic`）。
- 所有 `pub` 项必须有中文 `///` 文档（`missing_docs` 已 `deny`）。
- 集成测试**必须离线运行**，不触碰真实网络。
- 保持**确定性与零依赖**：不得引入墙钟、随机数、后台线程或第三方 crate；
  新增依赖视为架构变更，需先升级讨论。
- 触发行为必须只依赖显式传入的 `now_ms`，保证同一输入序列可复现。
- 非法 ID / 调度表达式必须在插入前 fail-closed，不得留下半成品条目。

## 提交前自检清单

- [ ] `cargo fmt --all -- --check` 通过
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] `cargo test --all-targets` 通过
- [ ] `cargo package --no-verify --allow-dirty` 通过
- [ ] 新增 `pub` 项都有中文 `///` 文档
- [ ] 文档中无「可独立发布」/ crates.io / docs.rs 表述
- [ ] 未引入任何新依赖，且未让触发行为依赖系统时间

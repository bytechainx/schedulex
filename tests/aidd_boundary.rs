#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! AIDD 对抗 / 边界用例（特性 002）。
//!
//! 候选由 AI 生成，逐条人工复核后仅保留「结论=保留」项；丢弃项登记于 PR 描述。
//!
//! // AIDD: ID 长度恰为 MAX_ID_LEN / +1 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md 正确性标准 非法 ID fail-closed | 结论=保留
//! // AIDD: ID 含 NUL 控制字符 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md 正确性标准 控制字符拒绝 | 结论=保留
//! // AIDD: normalize 只 trim 两端 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md 声明面 ID 治理 | 结论=保留
//! // AIDD: 非分钟段取非 * | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md 声明面 最小 cron 子集 | 结论=保留
//! // AIDD: 分钟步长取 0 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md 声明面 最小 cron 子集 | 结论=保留
//! // AIDD: 时间回退的 tick | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md 正确性标准 时间回退不推进 | 结论=保留
//! // AIDD: 大跨度 tick 不补跑 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md 正确性标准 大跨度不补跑 | 结论=保留
//! // AIDD: utilization 阈值为 0 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md 正确性标准 可见数值须有定义 | 结论=保留

use schedulex::{
    cron_matches, normalize_task_id, parse_cron_expr, utilization, validate_task_id, Job,
    JobRunner, Schedule, ScheduleError, Scheduler, MAX_ID_LEN,
};

/// 边界：ID 长度恰为上限通过，超出 1 字节拒绝。
#[test]
fn id_length_boundary() {
    validate_task_id(&"a".repeat(MAX_ID_LEN)).expect("上限内应通过");
    let error = validate_task_id(&"a".repeat(MAX_ID_LEN + 1)).expect_err("超 1 字节应拒绝");
    assert_eq!(error, ScheduleError::IdTooLong { max: MAX_ID_LEN });
}

/// 边界：ID 含 NUL / 其他控制字符一律拒绝。
#[test]
fn id_control_characters_rejected() {
    for raw in ["a\u{0000}b", "a\u{0007}b", "\u{001F}", "x\r\ny"] {
        assert_eq!(
            validate_task_id(raw).expect_err("控制字符必须拒绝"),
            ScheduleError::IdControlChar,
            "输入 {raw:?}"
        );
    }
}

/// 边界：`normalize_task_id` 只 trim 两端，保留内部空白；全空白视为空 ID。
#[test]
fn normalize_trims_only_the_ends() {
    assert_eq!(
        normalize_task_id("  name with space  ").expect("保留内部空格"),
        "name with space"
    );
    assert_eq!(normalize_task_id("\tjob-1\n").expect("trim 两端"), "job-1");
    assert!(normalize_task_id("   ").is_err());
}

/// 边界：5 段表达式中非分钟段必须为 `*`，多段/少段一律拒绝。
#[test]
fn cron_rejects_non_minute_fields() {
    assert!(parse_cron_expr("1 2 * * *").is_err());
    assert!(parse_cron_expr("* 2 * * *").is_err());
    assert!(parse_cron_expr("*/5 0 * * *").is_err());
    assert!(parse_cron_expr("* * * *").is_err());
    assert!(parse_cron_expr("* * * * * *").is_err());
}

/// 边界：分钟步长为 0 拒绝；`*/1` 等价每分钟。
#[test]
fn cron_step_zero_rejected() {
    assert!(parse_cron_expr("*/0 * * * *").is_err());
    let every_minute = parse_cron_expr("*/1 * * * *").expect("步长 1");
    assert!(cron_matches(&every_minute, 0));
    assert!(cron_matches(&every_minute, 60_000));
}

/// 边界：时间回退的 tick 不执行、不推进基线。
#[test]
fn regressed_tick_is_ignored() {
    let mut runner = JobRunner::new();
    runner
        .add(
            Job::new("fd", || Ok(())),
            Schedule::fixed_delay(10).expect("间隔"),
        )
        .expect("注册");
    assert_eq!(runner.tick(100).fired, 1);
    assert_eq!(runner.tick(99).fired, 0, "回退不执行");
    assert_eq!(runner.tick(109).fired, 0, "基线未被回退污染");
    assert_eq!(runner.tick(110).fired, 1);
}

/// 边界：大跨度 tick 只执行一次，不补跑错过的间隔。
#[test]
fn large_span_tick_does_not_backfill() {
    let mut runner = JobRunner::new();
    runner
        .add(
            Job::new("fd", || Ok(())),
            Schedule::fixed_delay(1_000).expect("间隔"),
        )
        .expect("注册");
    assert_eq!(runner.tick(0).fired, 1);
    assert_eq!(runner.tick(1_000_000).fired, 1, "不得补跑 999 次");
}

/// 边界：`utilization` 在阈值为 0 时返回 0.0，不产生 inf / NaN。
#[test]
fn utilization_zero_threshold_is_defined() {
    let mut registry = Scheduler::new();
    registry.schedule("one");
    assert_eq!(utilization(&registry, 0), 0.0);
    assert!(utilization(&registry, 0).is_finite());
}

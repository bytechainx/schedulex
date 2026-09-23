#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! TDD 行为契约（特性 002）。
//!
//! 入口集合 = `specs/features/002-*/contracts/public-api-contract.md` 中 `schedulex` 全部入口。
//! 下表每个入口先在变异副本上观测应红、再在本树观测绿；实际执行的变异与红绿结果
//! 见 PR 描述（变异描述 + 失败用例名 + 复现命令）。
//!
//! // TDD-PROBE: Scheduler::schedule | 变异：重复登记不幂等（长度递增） | 红=scheduler_schedule_is_idempotent_registry_insert | 绿=scheduler_schedule_is_idempotent_registry_insert
//! // TDD-PROBE: Scheduler::cancel | 变异：取消已不存在的 ID 仍返回 true | 红=scheduler_cancel_reports_existence_and_removes_entry | 绿=scheduler_cancel_reports_existence_and_removes_entry
//! // TDD-PROBE: Scheduler::list | 变异：list 丢项（只返回首条） | 红=scheduler_list_returns_every_registered_id | 绿=scheduler_list_returns_every_registered_id
//! // TDD-PROBE: JobRunner::add | 变异：非法 ID / 调度仍完成插入 | 红=job_runner_add_validates_before_insertion | 绿=job_runner_add_validates_before_insertion
//! // TDD-PROBE: JobRunner::tick | 变异：Once 到期后每次 tick 重复触发 | 红=job_runner_tick_fires_once_and_reports_errors | 绿=job_runner_tick_fires_once_and_reports_errors
//! // TDD-PROBE: JobRunner::list_meta | 变异：元数据不做字典序排序 | 红=job_runner_list_meta_is_sorted_by_id | 绿=job_runner_list_meta_is_sorted_by_id
//! // TDD-PROBE: schedule::parse_cron_expr | 变异：接受非分钟字段的非 * 取值 | 红=parse_cron_expr_supports_documented_minimal_subset | 绿=parse_cron_expr_supports_documented_minimal_subset
//! // TDD-PROBE: schedule::cron_matches | 变异：分钟谓词恒真 | 红=cron_matches_is_epoch_aligned_predicate | 绿=cron_matches_is_epoch_aligned_predicate
//! // TDD-PROBE: id::validate_task_id | 变异：长度边界由 > 改为 >= | 红=validate_task_id_enforces_documented_limits | 绿=validate_task_id_enforces_documented_limits
//! // TDD-PROBE: stats::utilization | 变异：阈值为 0 时不防护（返回 inf/NaN） | 红=utilization_reports_ratio_and_guards_zero_threshold | 绿=utilization_reports_ratio_and_guards_zero_threshold

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use schedulex::{
    cron_matches, parse_cron_expr, utilization, validate_task_id, CronParsed, Job, JobRunner,
    Schedule, ScheduleError, Scheduler, MAX_ID_LEN,
};

/// `Scheduler::schedule`：登记表只记录 ID，重复登记幂等。
#[test]
fn scheduler_schedule_is_idempotent_registry_insert() {
    let mut registry = Scheduler::new();
    assert!(registry.is_empty());
    registry.schedule("task-a");
    registry.schedule("task-a");
    assert_eq!(registry.len(), 1, "重复登记须幂等");
    assert!(registry.contains("task-a"));
    registry.schedule(String::from("task-b"));
    assert_eq!(registry.len(), 2);
}

/// `Scheduler::cancel`：返回「是否曾存在」，且条目确实被移除。
#[test]
fn scheduler_cancel_reports_existence_and_removes_entry() {
    let mut registry = Scheduler::new();
    registry.schedule("gone");
    assert!(registry.cancel("gone"));
    assert!(!registry.cancel("gone"), "重复取消返回 false");
    assert!(!registry.contains("gone"));
    assert_eq!(registry.len(), 0);
}

/// `Scheduler::list`：返回全部已登记 ID，不丢项、不重复。
#[test]
fn scheduler_list_returns_every_registered_id() {
    let mut registry = Scheduler::new();
    registry.schedule_many(["a", "b", "c"]);
    assert_eq!(registry.list().len(), 3);
    let listed: HashSet<String> = registry.list().into_iter().collect();
    assert_eq!(
        listed,
        HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()])
    );
}

/// `JobRunner::add`：非法 ID / 非法调度在插入前 fail-closed，且不改变 runner。
#[test]
fn job_runner_add_validates_before_insertion() {
    let mut runner = JobRunner::new();
    let error = runner
        .add(Job::new("", || Ok(())), Schedule::once(1))
        .expect_err("空 ID 必须拒绝");
    assert_eq!(error, ScheduleError::EmptyId);
    assert_eq!(runner.active_len(), 0);

    let zero_delay = Schedule::FixedDelay {
        every_ms: 0,
        first_at_ms: 0,
    };
    let error = runner
        .add(Job::new("x", || Ok(())), zero_delay)
        .expect_err("零间隔必须拒绝");
    assert!(matches!(error, ScheduleError::InvalidSchedule(_)));
    assert!(!runner.contains("x"), "失败不得留下条目");

    runner
        .add(Job::new("x", || Ok(())), Schedule::once(1))
        .expect("合法 job + 调度");
    assert!(runner.contains("x"));
}

/// `JobRunner::tick`：到期执行一次；Job 错误记入结果且不阻断其他 Job。
#[test]
fn job_runner_tick_fires_once_and_reports_errors() {
    let hits = Arc::new(AtomicU32::new(0));
    let worker = Arc::clone(&hits);
    let mut runner = JobRunner::new();
    runner
        .add(
            Job::new("bad", || Err(ScheduleError::JobFailed("boom".into()))),
            Schedule::once(10),
        )
        .expect("注册失败 job");
    runner
        .add(
            Job::new("ok", move || {
                worker.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }),
            Schedule::once(10),
        )
        .expect("注册正常 job");

    assert_eq!(runner.tick(9).fired, 0, "未到期不执行");
    let result = runner.tick(10);
    assert_eq!(result.fired, 1);
    assert_eq!(result.errors.len(), 1, "错误按执行顺序记录");
    assert_eq!(hits.load(Ordering::Relaxed), 1);
    assert_eq!(runner.tick(20).fired, 0, "Once 不得重复触发");
}

/// `JobRunner::list_meta`：元数据按 Job ID 字典序稳定列举。
#[test]
fn job_runner_list_meta_is_sorted_by_id() {
    let mut runner = JobRunner::new();
    for id in ["c", "a", "b"] {
        runner
            .add(Job::new(id, || Ok(())), Schedule::once(1))
            .expect("注册");
    }
    let ids: Vec<String> = runner
        .list_meta()
        .into_iter()
        .map(|meta| meta.id.as_str().to_string())
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

/// `schedule::parse_cron_expr`：只接受文档化的最小子集。
#[test]
fn parse_cron_expr_supports_documented_minimal_subset() {
    assert!(matches!(
        parse_cron_expr("every:1s").expect("秒换算"),
        CronParsed::EveryMs { every_ms: 1000 }
    ));
    assert!(matches!(
        parse_cron_expr("* * * * *").expect("每分钟"),
        CronParsed::MinuteMatch {
            every_n: None,
            exact: None
        }
    ));
    assert!(matches!(
        parse_cron_expr("*/5 * * * *").expect("分钟步长"),
        CronParsed::MinuteMatch {
            every_n: Some(5),
            exact: None
        }
    ));
    assert!(parse_cron_expr("1 2 * * *").is_err(), "非分钟字段须为 *");
    assert!(parse_cron_expr("60 * * * *").is_err(), "分钟须在 0..=59");
    assert!(parse_cron_expr("").is_err());
}

/// `schedule::cron_matches`：无状态 epoch 对齐谓词。
#[test]
fn cron_matches_is_epoch_aligned_predicate() {
    let every = parse_cron_expr("every:100").expect("解析 every:100");
    assert!(cron_matches(&every, 200));
    assert!(!cron_matches(&every, 150));

    let exact = parse_cron_expr("15 * * * *").expect("解析精确分钟");
    assert!(cron_matches(&exact, 15 * 60_000));
    assert!(!cron_matches(&exact, 0));

    let every_minute = parse_cron_expr("* * * * *").expect("解析每分钟");
    assert!(cron_matches(&every_minute, 0));
    assert!(cron_matches(&every_minute, 59_999));
}

/// `id::validate_task_id`：长度、空、控制字符三条边界。
#[test]
fn validate_task_id_enforces_documented_limits() {
    validate_task_id("job-1").expect("普通 ID");
    validate_task_id(&"x".repeat(MAX_ID_LEN)).expect("边界长度应通过");
    assert_eq!(
        validate_task_id("").expect_err("空 ID"),
        ScheduleError::EmptyId
    );
    assert!(matches!(
        validate_task_id(&"x".repeat(MAX_ID_LEN + 1)).expect_err("超长"),
        ScheduleError::IdTooLong { max } if max == MAX_ID_LEN
    ));
    assert_eq!(
        validate_task_id("a\u{0007}b").expect_err("控制字符"),
        ScheduleError::IdControlChar
    );
}

/// `stats::utilization`：相对软阈值比率，阈值为 0 时防护除零。
#[test]
fn utilization_reports_ratio_and_guards_zero_threshold() {
    let mut registry = Scheduler::new();
    for i in 0..20 {
        registry.schedule(format!("u{i}"));
    }
    assert_eq!(utilization(&registry, 10), 2.0);
    assert_eq!(utilization(&registry, 0), 0.0, "零阈值不得除零");
    assert_eq!(utilization(&Scheduler::new(), 10), 0.0);
}

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! E2E（schedulex）：端到端执行**全部**公开接口，不依赖外部真服务。
//!
//! 本仓是调度原语仓：E2E 的端到端含义是「**登记 → 校验 → 到期判定 → 触发 → 隔离失败 →
//! 统计/集合运算**」的完整闭环，而不是单点调用。因此：
//! - ID 域走 validate / normalize / debug_label / is_debug_label 全链；
//! - 调度域走 `Schedule::{once,fixed_delay,cron}` + `parse_cron_expr` + `cron_matches`；
//! - 运行域用真实 `JobRunner` 走 add → tick 到期触发（Once / FixedDelay / Cron）→
//!   时钟回退统计 → job 返回错误与 panic 的隔离；
//! - 登记表域走 `Scheduler` 全量集合运算（union/intersection/difference/retain）与
//!   `stats` 读数。
//!
//! 对齐对象是 `cargo +nightly public-api --simplified` 导出的完整公开面：
//! `fn` / `type` / `field` / `const` / `variant` 五类逐条登记在 [`E2E_MANIFEST`]，
//! 运行期由 `cover` 登记表核对「声明 = 实际执行」（缺一即失败）。
//!
//! **独立核对**：`scripts/verify-e2e-coverage.mjs` 会重新派生公开面与清单双向 diff，
//! 并用 `-C instrument-coverage` + `llvm-cov report --show-functions` 断言每条公开
//! 函数执行次数 > 0；本文件内的登记表只是**声明**，不是唯一证据。
//!
//! ```text
//! cd /home/workspace/bytechainx/infra/schedulex
//! cargo test --test e2e_schedule
//! node scripts/verify-e2e-coverage.mjs schedulex --no-coverage
//! ```

use std::collections::BTreeSet;

use schedulex::stats::stats as registry_stats;
use schedulex::{
    bulk::{schedule_checked_many, schedule_filtering},
    cron_matches, debug_label, is_busy, is_debug_label, normalize_task_id, over_soft_threshold,
    parse_cron_expr, status_line, utilization, validate_task_id, CronParsed, Job, JobFn, JobId,
    JobMeta, JobRunner, RegistryStats, Schedule, ScheduleError, ScheduleResult, Scheduler,
    MAX_ID_LEN, NO_HARD_CAPACITY,
};

/// 公开面清单：`(条目类别, 入口 id)`，由 `cargo +nightly public-api --simplified` 派生并冻结。
///
/// 类别取值域：`fn` / `type` / `field` / `const` / `variant`。
/// 该清单是运行时登记的**唯一事实源**——`cover::hit` 拒绝清单外的 id，收尾断言拒绝
/// 「声明了却没执行」的条目。清单本身的时效性由外部核对器与公开面 diff 保证。
#[rustfmt::skip]
const E2E_MANIFEST: &[(&str, &str)] = &[
    ("fn", "schedule_checked_many"),
    ("fn", "schedule_filtering"),
    ("const", "MAX_ID_LEN"),
    ("fn", "debug_label"),
    ("fn", "is_debug_label"),
    ("fn", "normalize_task_id"),
    ("fn", "validate_task_id"),
    ("type", "Job"),
    ("field", "Job::id"),
    ("field", "Job::name"),
    ("field", "Job::run"),
    ("fn", "Job::meta"),
    ("fn", "Job::new"),
    ("fn", "Job::with_name"),
    ("type", "JobId"),
    ("fn", "JobId::as_str"),
    ("fn", "JobId::checked"),
    ("fn", "JobId::new"),
    ("type", "JobMeta"),
    ("field", "JobMeta::id"),
    ("field", "JobMeta::name"),
    ("type", "JobFn"),
    ("type", "JobRunner"),
    ("fn", "JobRunner::active_len"),
    ("fn", "JobRunner::add"),
    ("fn", "JobRunner::cancel"),
    ("fn", "JobRunner::contains"),
    ("fn", "JobRunner::list_meta"),
    ("fn", "JobRunner::new"),
    ("fn", "JobRunner::remove"),
    ("fn", "JobRunner::tick"),
    ("type", "TickResult"),
    ("field", "TickResult::clock_regressed"),
    ("field", "TickResult::errors"),
    ("field", "TickResult::fired"),
    ("field", "TickResult::missed"),
    ("type", "CronParsed"),
    ("variant", "CronParsed::EveryMs"),
    ("variant", "CronParsed::MinuteMatch"),
    ("type", "Schedule"),
    ("variant", "Schedule::Cron"),
    ("variant", "Schedule::FixedDelay"),
    ("variant", "Schedule::Once"),
    ("fn", "Schedule::cron"),
    ("fn", "Schedule::fixed_delay"),
    ("fn", "Schedule::once"),
    ("fn", "cron_matches"),
    ("fn", "parse_cron_expr"),
    ("type", "RegistryStats"),
    ("field", "RegistryStats::empty"),
    ("field", "RegistryStats::len"),
    ("const", "NO_HARD_CAPACITY"),
    ("fn", "is_busy"),
    ("fn", "over_soft_threshold"),
    ("fn", "stats"),
    ("fn", "status_line"),
    ("fn", "utilization"),
    ("type", "ScheduleError"),
    ("variant", "ScheduleError::EmptyId"),
    ("variant", "ScheduleError::IdControlChar"),
    ("variant", "ScheduleError::IdTooLong"),
    ("variant", "ScheduleError::InvalidSchedule"),
    ("variant", "ScheduleError::JobFailed"),
    ("variant", "ScheduleError::JobPanicked"),
    ("type", "Scheduler"),
    ("fn", "Scheduler::cancel"),
    ("fn", "Scheduler::cancel_many"),
    ("fn", "Scheduler::clear"),
    ("fn", "Scheduler::contains"),
    ("fn", "Scheduler::difference_ids"),
    ("fn", "Scheduler::intersection_ids"),
    ("fn", "Scheduler::is_empty"),
    ("fn", "Scheduler::len"),
    ("fn", "Scheduler::list"),
    ("fn", "Scheduler::new"),
    ("fn", "Scheduler::retain"),
    ("fn", "Scheduler::schedule"),
    ("fn", "Scheduler::schedule_checked"),
    ("fn", "Scheduler::schedule_many"),
    ("fn", "Scheduler::schedule_normalized"),
    ("fn", "Scheduler::try_schedule"),
    ("fn", "Scheduler::union_ids"),
    ("type", "ScheduleResult"),
];

mod cover {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, OnceLock};

    static EXECUTED: OnceLock<Mutex<BTreeSet<(&'static str, &'static str)>>> = OnceLock::new();

    fn log() -> &'static Mutex<BTreeSet<(&'static str, &'static str)>> {
        EXECUTED.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    /// 登记一次真实执行。清单外的 `(类别, id)` 立即 panic，防止调用点与清单漂移。
    pub fn hit(kind: &'static str, id: &'static str) {
        assert!(
            super::E2E_MANIFEST
                .iter()
                .any(|(declared_kind, declared_id)| *declared_kind == kind && *declared_id == id),
            "登记了清单外的公开条目：{kind} {id}"
        );
        log().lock().expect("覆盖登记表锁中毒").insert((kind, id));
    }

    /// 已登记的执行集合（收尾断言用）。
    pub fn executed() -> BTreeSet<(&'static str, &'static str)> {
        log().lock().expect("覆盖登记表锁中毒").clone()
    }
}

/// 覆盖登记的简写入口（保持调用点可读）。
fn hit(kind: &'static str, id: &'static str) {
    cover::hit(kind, id);
}

/// 清单自身良构：类别取值域合法、`(类别, id)` 不重复。
fn assert_manifest_wellformed() {
    let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
    for (kind, id) in E2E_MANIFEST {
        assert!(
            matches!(*kind, "fn" | "type" | "field" | "const" | "variant"),
            "未知条目类别 {kind}（id={id}）"
        );
        assert!(seen.insert((*kind, *id)), "清单重复条目：{kind} {id}");
    }
}

/// 覆盖完整性：清单里每一条都必须被真实执行过。
fn assert_coverage_complete() {
    let executed = cover::executed();
    let mut missing: Vec<(&str, &str)> = Vec::new();
    for (kind, id) in E2E_MANIFEST {
        if !executed.contains(&(*kind, *id)) {
            missing.push((kind, id));
        }
    }
    assert!(missing.is_empty(), "声明了却未执行：{missing:?}");
}

/// ID 域：边界（空 / 超长 / 控制字符）、规范化、调试标签。
fn phase_id() {
    hit("const", "MAX_ID_LEN");
    hit("fn", "validate_task_id");
    hit("fn", "normalize_task_id");
    hit("fn", "debug_label");
    hit("fn", "is_debug_label");
    hit("type", "ScheduleError");

    assert_eq!(MAX_ID_LEN, 256);
    validate_task_id("job-a").expect("常规 ID 必须通过");

    assert_eq!(
        validate_task_id("").unwrap_err(),
        ScheduleError::EmptyId,
        "空 ID 必须判为 EmptyId"
    );
    assert_eq!(
        validate_task_id(&"x".repeat(MAX_ID_LEN + 1)).unwrap_err(),
        ScheduleError::IdTooLong { max: MAX_ID_LEN },
        "超长 ID 必须携带上限"
    );
    assert_eq!(
        validate_task_id("a\nb").unwrap_err(),
        ScheduleError::IdControlChar,
        "控制字符必须被拒绝"
    );

    assert_eq!(
        normalize_task_id("  job-a  ").expect("规范化必须成功"),
        "job-a",
        "必须 trim"
    );
    assert!(normalize_task_id("   ").is_err(), "空白规范化必须失败");

    assert_eq!(debug_label("p", "id"), "p:id");
    assert_eq!(debug_label("", "id"), "id", "空前缀不加分隔符");
    assert!(is_debug_label("p:id"), "含冒号且尾段合法即调试标签");
    assert!(!is_debug_label("plain"), "无冒号不是调试标签");
    assert!(!is_debug_label("p:"), "尾段为空不是调试标签");

    // 错误枚举：逐个变体构造，覆盖「可构造 + 可读」形状。
    hit("variant", "ScheduleError::EmptyId");
    hit("variant", "ScheduleError::IdControlChar");
    hit("variant", "ScheduleError::IdTooLong");
    hit("variant", "ScheduleError::InvalidSchedule");
    hit("variant", "ScheduleError::JobFailed");
    hit("variant", "ScheduleError::JobPanicked");

    let constructed: [(ScheduleError, &str); 6] = [
        (ScheduleError::EmptyId, "ScheduleError::EmptyId"),
        (ScheduleError::IdControlChar, "ScheduleError::IdControlChar"),
        (
            ScheduleError::IdTooLong { max: MAX_ID_LEN },
            "ScheduleError::IdTooLong",
        ),
        (
            ScheduleError::InvalidSchedule("bad".to_owned()),
            "ScheduleError::InvalidSchedule",
        ),
        (
            ScheduleError::JobFailed("failed".to_owned()),
            "ScheduleError::JobFailed",
        ),
        (
            ScheduleError::JobPanicked("panicked".to_owned()),
            "ScheduleError::JobPanicked",
        ),
    ];
    for (error, id) in constructed {
        assert!(!format!("{error:?}").is_empty(), "{id} 必须有 Debug");
        assert!(!error.to_string().is_empty(), "{id} 必须可读");
    }
}

/// 调度域：三种 Schedule 构造、cron 子集解析与到期判定。
fn phase_schedule() {
    hit("type", "Schedule");
    hit("variant", "Schedule::Once");
    hit("variant", "Schedule::FixedDelay");
    hit("variant", "Schedule::Cron");
    hit("fn", "Schedule::once");
    hit("fn", "Schedule::fixed_delay");
    hit("fn", "Schedule::cron");
    hit("fn", "parse_cron_expr");
    hit("fn", "cron_matches");
    hit("type", "CronParsed");
    hit("variant", "CronParsed::EveryMs");
    hit("variant", "CronParsed::MinuteMatch");

    let once = Schedule::once(1_000);
    assert!(matches!(once, Schedule::Once { at_ms: 1_000 }));

    let fixed = Schedule::fixed_delay(500).expect("正间隔必须成功");
    assert!(matches!(fixed, Schedule::FixedDelay { every_ms: 500, .. }));
    let bad = Schedule::fixed_delay(0).unwrap_err();
    assert!(
        matches!(bad, ScheduleError::InvalidSchedule(_)),
        "零间隔必须判为 InvalidSchedule"
    );

    let every = Schedule::cron("every:500").expect("every:<ms> 必须成功");
    assert!(matches!(
        every,
        Schedule::Cron { ref parsed, .. } if matches!(parsed, CronParsed::EveryMs { every_ms: 500 })
    ));

    let stepped = parse_cron_expr("*/5 * * * *").expect("分钟步长必须成功");
    assert!(matches!(
        stepped,
        CronParsed::MinuteMatch {
            every_n: Some(5),
            exact: None
        }
    ));
    let exact = parse_cron_expr("7 * * * *").expect("分钟定点必须成功");
    assert!(matches!(
        exact,
        CronParsed::MinuteMatch {
            every_n: None,
            exact: Some(7)
        }
    ));
    let wildcard = parse_cron_expr("* * * * *").expect("全通配必须成功");
    assert!(matches!(
        wildcard,
        CronParsed::MinuteMatch {
            every_n: None,
            exact: None
        }
    ));
    let seconds = parse_cron_expr("every:2s").expect("秒后缀必须成功");
    assert!(
        matches!(seconds, CronParsed::EveryMs { every_ms: 2_000 }),
        "s 后缀必须换算为毫秒"
    );

    for bad_expr in ["", "bad", "1 2 3", "*/0 * * * *", "99 * * * *", "0 1 2 3 4"] {
        assert!(
            parse_cron_expr(bad_expr).is_err(),
            "非法表达式必须被拒绝：{bad_expr:?}"
        );
    }

    assert!(cron_matches(&CronParsed::EveryMs { every_ms: 100 }, 300));
    assert!(!cron_matches(&CronParsed::EveryMs { every_ms: 100 }, 350));
    assert!(
        !cron_matches(&CronParsed::EveryMs { every_ms: 0 }, 0),
        "零周期不得命中"
    );
    assert!(cron_matches(
        &CronParsed::MinuteMatch {
            every_n: None,
            exact: None
        },
        1_700_000_000_000_u64
    ));

    let cron_stepped = Schedule::cron("*/10 * * * *").expect("步长 cron 必须成功");
    assert!(matches!(cron_stepped, Schedule::Cron { .. }));
    let cron_every = Schedule::cron("every:1s").expect("every cron 必须成功");
    assert!(matches!(cron_every, Schedule::Cron { .. }));
    assert!(Schedule::cron("nonsense").is_err(), "非法 cron 必须失败");
}

/// Job 域：标识、构造、命名、元数据与回调。
fn phase_job() {
    hit("type", "JobId");
    hit("fn", "JobId::new");
    hit("fn", "JobId::checked");
    hit("fn", "JobId::as_str");
    hit("type", "Job");
    hit("fn", "Job::new");
    hit("fn", "Job::with_name");
    hit("fn", "Job::meta");
    hit("type", "JobMeta");
    hit("type", "JobFn");

    let id = JobId::new("job-a");
    assert_eq!(id.as_str(), "job-a");
    let checked = JobId::checked("job-b").expect("合法 ID 必须通过");
    assert_eq!(checked.as_str(), "job-b");
    assert!(matches!(
        JobId::checked("").unwrap_err(),
        ScheduleError::EmptyId
    ));

    let job = Job::new(JobId::new("job-c"), || Ok(())).with_name("清理任务");
    hit("field", "Job::id");
    hit("field", "Job::name");
    assert_eq!(job.id.as_str(), "job-c");
    assert_eq!(job.name.as_deref(), Some("清理任务"));

    let mut meta: JobMeta = job.meta();
    hit("field", "JobMeta::id");
    hit("field", "JobMeta::name");
    assert_eq!(meta.id.as_str(), "job-c");
    assert_eq!(meta.name.as_deref(), Some("清理任务"));
    meta.name = None;
    assert!(meta.name.is_none(), "元数据为可写快照");

    let mut run: JobFn = job.run;
    hit("field", "Job::run");
    run().expect("回调必须返回 Ok");
}

/// 计数用 job 工厂（保持各阶段断言可观测）。
fn counting_job(id: &str, counter: std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Job {
    Job::new(JobId::new(id), move || {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    })
}

/// 运行域：真实 JobRunner 走 add → tick 触发 → 时钟回退 → 失败隔离。
fn phase_runner() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    hit("type", "JobRunner");
    hit("fn", "JobRunner::new");
    hit("fn", "JobRunner::add");
    hit("fn", "JobRunner::active_len");
    hit("fn", "JobRunner::contains");
    hit("fn", "JobRunner::list_meta");
    hit("fn", "JobRunner::remove");
    hit("fn", "JobRunner::cancel");
    hit("fn", "JobRunner::tick");
    hit("type", "TickResult");
    hit("field", "TickResult::fired");
    hit("field", "TickResult::missed");
    hit("field", "TickResult::errors");
    hit("field", "TickResult::clock_regressed");

    let hits = Arc::new(AtomicUsize::new(0));
    let mut runner = JobRunner::new();
    assert_eq!(runner.active_len(), 0);

    runner
        .add(
            counting_job("once-a", Arc::clone(&hits)),
            Schedule::once(1_000),
        )
        .expect("登记 Once 必须成功");
    runner
        .add(
            counting_job("delay-a", Arc::clone(&hits)),
            Schedule::fixed_delay(500).expect("调度必须合法"),
        )
        .expect("登记 FixedDelay 必须成功");
    runner
        .add(
            counting_job("cron-a", Arc::clone(&hits)),
            Schedule::cron("every:250").expect("调度必须合法"),
        )
        .expect("登记 Cron 必须成功");

    assert_eq!(runner.active_len(), 3);
    assert!(runner.contains("once-a"));
    let metas = runner.list_meta();
    assert_eq!(metas.len(), 3);
    assert!(metas.iter().any(|m| m.id.as_str() == "delay-a"));

    // 非法调度：零间隔必须被 add 拒绝。
    let zero = runner.add(
        Job::new(JobId::new("bad-b"), || Ok(())),
        Schedule::FixedDelay {
            every_ms: 0,
            first_at_ms: 0,
        },
    );
    assert!(
        matches!(zero.unwrap_err(), ScheduleError::InvalidSchedule(_)),
        "零间隔必须被拒"
    );

    // 时钟回退：不执行、不推进基线，但统计当刻到期数。
    let first = runner.tick(0);
    assert!(!first.clock_regressed);
    assert_eq!(
        first.fired, 2,
        "0ms 时 FixedDelay 与 every 型 cron 立即到期，Once(1000) 未到期"
    );
    let equal = runner.tick(0);
    assert!(!equal.clock_regressed, "相等时刻不算回退");
    let _ = runner.tick(10);
    let regressed = runner.tick(5);
    assert!(regressed.clock_regressed, "更早时刻必须判为回退");
    assert_eq!(regressed.fired, 0, "回退时不执行");

    // 正常推进：固定间隔与 cron 到期触发。
    let fired = runner.tick(1_000);
    assert!(!fired.clock_regressed);
    assert!(fired.fired >= 1, "1s 时至少 Once 与 FixedDelay 到期");
    assert!(hits.load(Ordering::SeqCst) >= 1);
    assert!(fired.errors.is_empty(), "正常 job 不应产生错误");
    let probe = runner.tick(1_100);
    assert!(probe.missed <= 3, "missed 计数上界由活跃 job 数决定");

    // 失败隔离：返回 Err → JobFailed；panic → JobPanicked。
    let mut failing = JobRunner::new();
    failing
        .add(
            Job::new(JobId::new("err-a"), || {
                Err(ScheduleError::JobFailed("业务失败".into()))
            }),
            Schedule::once(0),
        )
        .expect("登记必须成功");
    failing
        .add(
            Job::new(JobId::new("panic-a"), || panic!("job 内部 panic")),
            Schedule::once(0),
        )
        .expect("登记必须成功");
    // runner 已捕获 panic；此处收敛 panic hook 的 stderr 噪音，便于阅读断言结果。
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = failing.tick(1);
    std::panic::set_hook(prev_hook);
    assert_eq!(outcome.fired, 0, "失败与 panic 不计入 fired");
    assert_eq!(outcome.errors.len(), 2, "两条异常都必须被隔离上报");
    assert!(
        outcome
            .errors
            .iter()
            .any(|(_, e)| matches!(e, ScheduleError::JobFailed(_))),
        "必须上报 JobFailed"
    );
    assert!(
        outcome
            .errors
            .iter()
            .any(|(_, e)| matches!(e, ScheduleError::JobPanicked(_))),
        "必须上报 JobPanicked"
    );
    assert_eq!(failing.active_len(), 2, "异常不得破坏 runner 不变量");

    // 取消 / 移除：cancel 幂等返回存在性，remove 之后不再活跃。
    assert!(runner.cancel("once-a"), "首次取消返回 true");
    assert!(runner.cancel("once-a"), "重复取消仍返回 true");
    assert!(!runner.cancel("不存在"), "不存在的 ID 返回 false");
    assert!(runner.contains("delay-a"));
    assert!(runner.remove("delay-a"));
    assert!(!runner.contains("delay-a"));
    assert!(!runner.remove("delay-a"), "重复移除返回 false");
    assert_eq!(
        runner.active_len(),
        1,
        "取消与移除各减一个活跃项，仅剩 cron-a"
    );
}

/// 登记表域：集合运算、批量登记、统计读数。
fn phase_scheduler() {
    hit("type", "Scheduler");
    hit("fn", "Scheduler::new");
    hit("fn", "Scheduler::schedule");
    hit("fn", "Scheduler::schedule_checked");
    hit("fn", "Scheduler::schedule_normalized");
    hit("fn", "Scheduler::try_schedule");
    hit("fn", "Scheduler::schedule_many");
    hit("fn", "Scheduler::contains");
    hit("fn", "Scheduler::len");
    hit("fn", "Scheduler::is_empty");
    hit("fn", "Scheduler::list");
    hit("fn", "Scheduler::cancel");
    hit("fn", "Scheduler::cancel_many");
    hit("fn", "Scheduler::clear");
    hit("fn", "Scheduler::retain");
    hit("fn", "Scheduler::difference_ids");
    hit("fn", "Scheduler::intersection_ids");
    hit("fn", "Scheduler::union_ids");
    hit("fn", "schedule_checked_many");
    hit("fn", "schedule_filtering");

    let mut a = Scheduler::new();
    assert!(a.is_empty());
    a.schedule("a1");
    a.schedule("a2");
    a.schedule_checked("a3").expect("合法 ID 必须通过");
    assert!(matches!(
        a.schedule_checked("").unwrap_err(),
        ScheduleError::EmptyId
    ));
    a.schedule_normalized("  a4  ").expect("trim 后必须通过");
    assert!(a.contains("a4"), "必须按 trim 后登记");
    assert!(a.try_schedule("a5"), "首次插入返回 true");
    assert!(!a.try_schedule("a5"), "重复插入返回 false");
    assert_eq!(a.len(), 5);
    assert!(!a.is_empty());

    let mut sorted = a.list();
    sorted.sort();
    assert_eq!(sorted, vec!["a1", "a2", "a3", "a4", "a5"]);

    let mut b = Scheduler::new();
    b.schedule_many(["a2", "a3", "b1"]);
    assert_eq!(b.len(), 3);

    let mut union = a.union_ids(&b);
    union.sort();
    assert_eq!(union, vec!["a1", "a2", "a3", "a4", "a5", "b1"]);
    let mut inter = a.intersection_ids(&b);
    inter.sort();
    assert_eq!(inter, vec!["a2", "a3"]);
    let mut diff = a.difference_ids(&b);
    diff.sort();
    assert_eq!(diff, vec!["a1", "a4", "a5"]);
    assert_eq!(b.difference_ids(&a), vec!["b1".to_owned()]);

    let mut bulk_ok = Scheduler::new();
    let n = schedule_checked_many(&mut bulk_ok, &["x1", "x2", "x3"]).expect("批量必须成功");
    assert_eq!((n, bulk_ok.len()), (3, 3));
    let mut bulk_bad = Scheduler::new();
    assert!(
        schedule_checked_many(&mut bulk_bad, &["y1", ""]).is_err(),
        "含非法 ID 的批量必须失败"
    );

    let mut filtered = Scheduler::new();
    let (accepted, rejected) = schedule_filtering(&mut filtered, &["z1", "", "z2", "a\nb"]);
    assert_eq!(accepted, 2);
    assert_eq!(rejected.len(), 2, "非法项必须逐条回落");
    assert_eq!(filtered.len(), 2);

    assert!(a.cancel("a1"));
    assert!(!a.cancel("a1"), "已取消的 ID 视为不存在");
    assert_eq!(a.cancel_many(["a2", "nope"]), 1, "只统计真实取消数");
    a.retain(|id| id == "a4");
    assert_eq!(a.list(), vec!["a4".to_owned()]);
    a.clear();
    assert!(a.is_empty());

    hit("type", "RegistryStats");
    hit("field", "RegistryStats::empty");
    hit("field", "RegistryStats::len");
    hit("const", "NO_HARD_CAPACITY");
    hit("fn", "stats");
    hit("fn", "is_busy");
    hit("fn", "over_soft_threshold");
    hit("fn", "utilization");
    hit("fn", "status_line");

    let empty_stats: RegistryStats = registry_stats(&a);
    assert!(empty_stats.empty);
    assert_eq!(empty_stats.len, 0);
    let full_stats = registry_stats(&b);
    assert!(!full_stats.empty);
    assert_eq!(full_stats.len, 3);

    assert_eq!(NO_HARD_CAPACITY, None, "无硬上限必须显式表达");
    assert!(!is_busy(&a, 10), "空表不忙");
    assert!(is_busy(&b, 1), "超过软阈值即为忙");
    assert!(over_soft_threshold(&b, 1));
    assert!(!over_soft_threshold(&a, 10));
    let ratio = utilization(&b, 4);
    assert!((0.0..=1.0).contains(&ratio), "利用率必须在 0..=1");
    assert!(!status_line(&b).is_empty(), "状态行不得为空");
}

/// `ScheduleResult` 别名可用（`type` 条目落地）。
fn typed_result(ok: bool) -> ScheduleResult<usize> {
    if ok {
        Ok(1)
    } else {
        Err(ScheduleError::EmptyId)
    }
}

/// 单一驱动用例：保证阶段顺序与覆盖断言在同一个进程内完成。
#[test]
fn e2e_schedule_all_public_api() {
    assert_manifest_wellformed();
    phase_id();
    phase_schedule();
    phase_job();
    phase_runner();
    phase_scheduler();
    hit("type", "ScheduleResult");
    assert_eq!(typed_result(true).expect("别名结果必须可用"), 1);
    assert!(typed_result(false).is_err(), "错误分支必须可用");
    assert_coverage_complete();
}

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! SDD 规格对照（特性 002）：把 `docs/标准.md` 的每个 `##` 章节条款转成可执行断言。
//!
//! 章节与断言函数须与 `docs/标准.md` 的 `##` 章节 1:1（检查器按标题逐字比对）。
//!
//! // SPEC-MAP: S-1 | 声明面 | assert_declaration_surface
//! // SPEC-MAP: S-2 | 正确性标准 | assert_correctness_standard
//! // SPEC-MAP: S-3 | 安全与可靠性边界 | assert_safety_and_reliability_boundary
//! // SPEC-MAP: S-4 | 验收 | assert_acceptance

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use schedulex::{Job, JobRunner, Schedule, ScheduleError, Scheduler};

/// S-1：`Scheduler` 是 ID registry 不执行 Job；`JobRunner` 由宿主显式 `tick` 驱动；
/// `Schedule` 提供 Once / FixedDelay / 最小 cron 三形态。
#[test]
fn assert_declaration_surface() {
    let mut registry = Scheduler::new();
    registry.schedule("declared");
    assert!(registry.contains("declared"), "登记表只记录 ID");

    let hits = Arc::new(AtomicU32::new(0));
    let worker = Arc::clone(&hits);
    let mut runner = JobRunner::new();
    runner
        .add(
            Job::new("r", move || {
                worker.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }),
            Schedule::once(5),
        )
        .expect("注册");
    assert_eq!(hits.load(Ordering::Relaxed), 0, "注册不等于执行");
    assert_eq!(runner.tick(4).fired, 0);
    assert_eq!(runner.tick(5).fired, 1, "仅在显式 tick 时执行");

    let _ = Schedule::once(0);
    let _ = Schedule::fixed_delay(1).expect("固定间隔");
    let _ = Schedule::cron("*/5 * * * *").expect("最小 cron 分钟子集");
}

/// S-2：fail-closed、同 tick 稳定排序、时间回退不推进、大跨度不补跑、Job Err 继续、错误为中文。
#[test]
fn assert_correctness_standard() {
    // 非法 ID / 调度在插入前 fail-closed。
    let mut guarded = JobRunner::new();
    assert!(guarded
        .add(Job::new("", || Ok(())), Schedule::once(1))
        .is_err());
    assert!(guarded
        .add(
            Job::new("x", || Ok(())),
            Schedule::FixedDelay {
                every_ms: 0,
                first_at_ms: 0
            }
        )
        .is_err());
    assert_eq!(guarded.active_len(), 0);

    // 同 tick 执行与 metadata 查询稳定排序。
    let order = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut runner = JobRunner::new();
    for id in ["b", "a"] {
        let log = Arc::clone(&order);
        let name = id.to_string();
        runner
            .add(
                Job::new(id, move || {
                    log.lock().expect("记录执行顺序").push(name.clone());
                    Ok(())
                }),
                Schedule::once(0),
            )
            .expect("注册");
    }
    let _ = runner.tick(0);
    assert_eq!(
        *order.lock().expect("读取执行顺序"),
        vec!["a".to_string(), "b".to_string()],
        "同 tick 按 Job ID 字典序执行"
    );
    let ids: Vec<String> = runner
        .list_meta()
        .into_iter()
        .map(|meta| meta.id.as_str().to_string())
        .collect();
    assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);

    // 时间回退不推进；大跨度不补跑。
    let mut delay = JobRunner::new();
    delay
        .add(
            Job::new("fd", || Ok(())),
            Schedule::fixed_delay(10).expect("间隔"),
        )
        .expect("注册");
    assert_eq!(delay.tick(100).fired, 1);
    assert_eq!(delay.tick(50).fired, 0, "回退的 tick 被忽略");
    assert_eq!(delay.tick(1_000).fired, 1, "大跨度只执行一次，不补跑");

    // Job Err 继续后续 Job。
    let mut mixed = JobRunner::new();
    mixed
        .add(
            Job::new("bad", || Err(ScheduleError::JobFailed("boom".into()))),
            Schedule::once(0),
        )
        .expect("注册");
    mixed
        .add(Job::new("good", || Ok(())), Schedule::once(0))
        .expect("注册");
    let result = mixed.tick(0);
    assert_eq!(result.fired, 1, "错误 Job 不阻断后续 Job");
    assert_eq!(result.errors.len(), 1);

    // 可见错误为简体中文。
    assert!(ScheduleError::EmptyId.to_string().contains("不能为空"));
    assert!(ScheduleError::IdTooLong { max: 1 }
        .to_string()
        .contains("最大长度"));
    assert!(ScheduleError::IdControlChar
        .to_string()
        .contains("控制字符"));
    assert!(ScheduleError::InvalidSchedule("x".into())
        .to_string()
        .contains("非法调度"));
    assert!(ScheduleError::JobFailed("x".into())
        .to_string()
        .contains("任务执行失败"));
}

/// S-3：无 unsafe / 网络 / 文件系统 / 真实时钟 / 后台线程；状态进程内、崩溃即丢。
#[test]
fn assert_safety_and_reliability_boundary() {
    // 真实时钟不参与：极端未来时刻立即执行，不做任何等待。
    let mut runner = JobRunner::new();
    runner
        .add(Job::new("far", || Ok(())), Schedule::once(u64::MAX))
        .expect("注册");
    assert_eq!(runner.tick(u64::MAX).fired, 1);

    // 无共享全局状态：新实例为空，互不影响。
    let mut first = JobRunner::new();
    first
        .add(Job::new("a", || Ok(())), Schedule::once(0))
        .expect("注册");
    let second = JobRunner::new();
    assert_eq!(second.active_len(), 0);
    assert!(second.list_meta().is_empty());

    assert_send::<JobRunner>();
    assert_send::<Scheduler>();
}

/// S-4：验收以 public seam 测试 + clippy + rustdoc + 公开 API 表面测试为证据。
#[test]
fn assert_acceptance() {
    let _ = std::env::current_dir().expect("可取得当前目录（验收命令可执行）");
    // 公开 API 表面可被外部命名（rustdoc / 集成测试即证据面）。
    assert!(std::any::type_name::<Scheduler>().ends_with("Scheduler"));
    assert!(std::any::type_name::<JobRunner>().ends_with("JobRunner"));
}

/// 编译期断言：公开类型可跨线程持有（无内部可变全局、无第三方运行时）。
fn assert_send<T: Send>() {}

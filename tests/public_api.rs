#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 公开 API 集成测试：从 crate 根调用 shipped 函数（非 re-implement oracle）。

use schedulex::Scheduler;
use std::collections::HashSet;

#[test]
fn public_schedule_list_cancel_roundtrip() {
    let mut s = Scheduler::new();
    s.schedule("alpha");
    s.schedule("beta");

    let set: HashSet<_> = s.list().into_iter().collect();
    assert_eq!(
        set,
        HashSet::from(["alpha".to_string(), "beta".to_string()])
    );

    assert!(s.cancel("alpha"));
    assert!(!s.cancel("alpha"));

    let left: HashSet<_> = s.list().into_iter().collect();
    assert_eq!(left, HashSet::from(["beta".to_string()]));
}

#[test]
fn public_default_empty_and_schedule_owned_string() {
    let mut s = Scheduler::default();
    assert!(s.list().is_empty());
    s.schedule(String::from("owned-id"));
    assert_eq!(s.list(), vec!["owned-id".to_string()]);
}

#[test]
fn public_registry_helpers_stats_bulk() {
    use schedulex::{
        is_busy, schedule_checked_many, schedule_filtering, stats, validate_task_id, ScheduleError,
        Scheduler, MAX_ID_LEN, NO_HARD_CAPACITY,
    };
    let mut s = Scheduler::new();
    assert_eq!(schedule_checked_many(&mut s, &["a", "b"]).unwrap(), 2);
    let (ok, bad) = schedule_filtering(&mut s, &["c", ""]);
    assert_eq!(ok, 1);
    assert_eq!(bad.len(), 1);
    assert!(is_busy(&s, 3));
    assert_eq!(stats(&s).len, 3);
    assert!(NO_HARD_CAPACITY.is_none());
    s.schedule_normalized("  d  ").unwrap();
    assert!(s.contains("d"));
    // 公开错误面：空 / 过长 / 控制字符 分型与 Display 不得混用「不能为空」
    assert_eq!(validate_task_id("").unwrap_err(), ScheduleError::EmptyId);
    let too_long = validate_task_id(&"z".repeat(MAX_ID_LEN + 1)).unwrap_err();
    assert_eq!(too_long, ScheduleError::IdTooLong { max: MAX_ID_LEN });
    assert!(format!("{too_long}").contains("最大长度"));
    assert!(!format!("{too_long}").contains("不能为空"));
    let ctrl = validate_task_id("a\nb").unwrap_err();
    assert_eq!(ctrl, ScheduleError::IdControlChar);
    assert!(format!("{ctrl}").contains("控制字符"));
}

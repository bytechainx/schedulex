#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 仅演示 `Scheduler` ID registry；本示例不会触发任何 Job。
//!
//! crate 另有宿主显式驱动的 `JobRunner::tick(now_ms)`，但仍无后台 timer、
//! 持久化或分布式调度能力。

use schedulex::Scheduler;

fn main() {
    let mut registry = Scheduler::new();
    registry.schedule("job-a");
    registry.schedule("job-b");
    registry.schedule("job-a");

    let mut ids = registry.list();
    ids.sort();
    assert_eq!(ids, vec!["job-a".to_string(), "job-b".to_string()]);

    assert!(registry.cancel("job-a"));
    assert!(!registry.cancel("job-a"));

    println!("schedulex_registry_ok remaining={:?}", registry.list());
}

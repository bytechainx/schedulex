#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 最小消费者路径：Scheduler registry + 宿主驱动 `JobRunner::tick` + profile 标记。
//!
//! ```bash
//! SCHEDULEX_LIVE_PROFILE=production cargo run -p schedulex --example schedulex_basic
//! ```

use schedulex::{Job, JobRunner, Schedule, Scheduler};

fn main() {
    let profile = std::env::var("SCHEDULEX_LIVE_PROFILE").unwrap_or_else(|_| "development".into());

    let mut registry = Scheduler::new();
    registry.schedule("reg-a");
    registry.schedule("reg-b");
    assert_eq!(registry.len(), 2);

    let mut runner = JobRunner::new();
    runner
        .add(Job::new("tick-job", || Ok(())), Schedule::once(100))
        .expect("add must succeed");

    let before = runner.tick(50);
    assert_eq!(before.fired, 0);

    let after = runner.tick(100);
    assert_eq!(after.fired, 1);

    println!(
        "schedulex-consumer: ok profile={profile} registry_len={} tick_fired={}",
        registry.len(),
        after.fired
    );
}

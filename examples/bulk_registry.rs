#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 批量登记（仍无定时触发）。
use schedulex::{schedule_checked_many, stats, Scheduler};

fn main() {
    let mut s = Scheduler::new();
    schedule_checked_many(&mut s, &["job-a", "job-b"]).expect("ok");
    println!("len={}", stats(&s).len);
    assert_eq!(s.len(), 2);
}

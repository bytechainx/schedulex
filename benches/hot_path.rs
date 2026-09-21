#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! schedulex 热路径：schedule/list/cancel。
use std::hint::black_box;
use std::time::Instant;

use schedulex::Scheduler;

fn iters() -> u32 {
    if std::env::args().any(|a| a == "--quick") {
        2_000
    } else {
        100_000
    }
}

fn main() {
    let n = iters();
    let mut s = Scheduler::new();
    for i in 0..n.min(20) {
        s.schedule(format!("t{i}"));
    }
    let start = Instant::now();
    for i in 0..n {
        let id = format!("job-{i}");
        s.schedule(&id);
        black_box(s.list().len());
        black_box(s.cancel(&id));
    }
    let elapsed = start.elapsed();
    println!(
        "bench_schedulex_registry: iters={n} total={elapsed:?} per_iter={:?}",
        elapsed / n
    );
}

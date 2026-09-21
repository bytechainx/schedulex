#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! schedulex 公开面：Debug/Clone + Default 路径。

use schedulex::Scheduler;
use std::collections::HashSet;

#[test]
fn scheduler_debug_clone_and_idempotent() {
    let mut s = Scheduler::new();
    s.schedule("a");
    s.schedule("a");
    let c = s.clone();
    assert_eq!(
        s.list().into_iter().collect::<HashSet<_>>(),
        c.list().into_iter().collect::<HashSet<_>>()
    );
    assert!(format!("{s:?}").contains("Scheduler"));
    assert!(!s.cancel("missing"));
    assert!(s.cancel("a"));
    assert!(Scheduler::default().list().is_empty());
}

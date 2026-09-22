//! schedulex —— 确定性任务调度原语：任务 ID 登记表 + 显式 tick 驱动的 Job 运行器。
//!
//! | 面 | 类型 | 说明 |
//! |----|------|------|
//! | 登记表 | [`Scheduler`] | 仅 ID 集合；登记 ≠ 执行 |
//! | 调度 | [`Schedule`] / [`JobRunner`] | `tick(now_ms)` 确定性触发；无墙钟依赖 |
//!
//! cron 仅支持文档化最小子集（见 [`schedule`] 模块）。
//! 本 crate 零依赖、不使用墙钟、不创建后台线程：所有时间推进都由宿主显式传入。
//!
//! # 最小示例
//!
//! ```
//! use schedulex::{Job, JobRunner, Schedule, ScheduleResult, Scheduler};
//!
//! fn main() -> ScheduleResult<()> {
//!     // 1) 登记表只登记 ID，不执行任何东西
//!     let mut registry = Scheduler::new();
//!     registry.schedule("job-1");
//!     assert!(registry.cancel("job-1"));
//!
//!     // 2) 运行器只在宿主显式 tick 时执行
//!     let mut runner = JobRunner::new();
//!     runner.add(Job::new("job-1", || Ok(())), Schedule::once(10))?;
//!     assert_eq!(runner.tick(9).fired, 0);
//!     assert_eq!(runner.tick(10).fired, 1);
//!     Ok(())
//! }
//! ```

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(unreachable_pub)]

use std::collections::HashMap;

pub mod bulk;
pub mod id;
pub mod job;
pub mod runner;
pub mod schedule;
pub mod stats;
pub use bulk::{schedule_checked_many, schedule_filtering};
pub use id::{debug_label, is_debug_label, normalize_task_id, validate_task_id, MAX_ID_LEN};
pub use job::{Job, JobFn, JobId, JobMeta};
pub use runner::{JobRunner, TickResult};
pub use schedule::{cron_matches, parse_cron_expr, CronParsed, Schedule};
pub use stats::{
    is_busy, over_soft_threshold, stats, status_line, utilization, RegistryStats, NO_HARD_CAPACITY,
};

/// crate 专用 `Result` 别名。
pub type ScheduleResult<T> = Result<T, ScheduleError>;

/// 任务 ID / 调度错误。
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    /// 空 ID。
    EmptyId,
    /// ID 超过最大允许字节长度。
    IdTooLong {
        /// 允许的最大字节数。
        max: usize,
    },
    /// ID 含控制字符。
    IdControlChar,
    /// 非法调度表达式或参数。
    InvalidSchedule(String),
    /// Job 执行失败。
    JobFailed(String),
    /// Job 回调 panic，已被 `tick` 捕获并视为该 Job 本次失败。
    JobPanicked(String),
}

impl std::fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyId => write!(f, "任务 ID 不能为空"),
            Self::IdTooLong { max } => write!(f, "任务 ID 超过最大长度 {max}"),
            Self::IdControlChar => write!(f, "任务 ID 不能包含控制字符"),
            Self::InvalidSchedule(msg) => write!(f, "非法调度: {msg}"),
            Self::JobFailed(msg) => write!(f, "任务执行失败: {msg}"),
            Self::JobPanicked(msg) => write!(f, "任务 panic 已捕获: {msg}"),
        }
    }
}

impl std::error::Error for ScheduleError {}

/// 内存任务 ID 登记表（与 [`JobRunner`] 独立；无自动联动）。
#[derive(Debug, Clone)]
pub struct Scheduler {
    tasks: HashMap<String, ()>,
}

impl Scheduler {
    /// 创建空登记表。
    #[must_use]
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    /// 登记任务 ID（重复幂等覆盖）。
    pub fn schedule(&mut self, id: impl Into<String>) {
        self.tasks.insert(id.into(), ());
    }

    /// 校验后登记。
    pub fn schedule_checked(&mut self, id: impl Into<String>) -> Result<(), ScheduleError> {
        let id = id.into();
        validate_task_id(&id)?;
        self.schedule(id);
        Ok(())
    }

    /// trim + 校验后登记。
    pub fn schedule_normalized(&mut self, id: &str) -> Result<(), ScheduleError> {
        let id = normalize_task_id(id)?;
        self.schedule(id);
        Ok(())
    }

    /// 仅当尚未登记时插入。
    pub fn try_schedule(&mut self, id: impl Into<String>) -> bool {
        let id = id.into();
        if self.tasks.contains_key(&id) {
            return false;
        }
        self.tasks.insert(id, ());
        true
    }

    /// 批量登记。
    pub fn schedule_many<I, S>(&mut self, ids: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for id in ids {
            self.schedule(id);
        }
    }

    /// 取消；返回是否曾存在。
    pub fn cancel(&mut self, id: &str) -> bool {
        self.tasks.remove(id).is_some()
    }

    /// 批量取消。
    pub fn cancel_many<'a, I>(&mut self, ids: I) -> usize
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut n = 0;
        for id in ids {
            if self.cancel(id) {
                n += 1;
            }
        }
        n
    }

    /// 是否已登记。
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.tasks.contains_key(id)
    }

    /// 条数。
    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// 是否为空。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// 清空。
    pub fn clear(&mut self) {
        self.tasks.clear();
    }

    /// 全部 ID（顺序未承诺）。
    #[must_use]
    pub fn list(&self) -> Vec<String> {
        self.tasks.keys().cloned().collect()
    }

    /// 交集。
    #[must_use]
    pub fn intersection_ids(&self, other: &Scheduler) -> Vec<String> {
        self.tasks
            .keys()
            .filter(|k| other.contains(k))
            .cloned()
            .collect()
    }

    /// 差集。
    #[must_use]
    pub fn difference_ids(&self, other: &Scheduler) -> Vec<String> {
        self.tasks
            .keys()
            .filter(|k| !other.contains(k))
            .cloned()
            .collect()
    }

    /// 并集。
    #[must_use]
    pub fn union_ids(&self, other: &Scheduler) -> Vec<String> {
        let mut m = self.tasks.clone();
        for k in other.tasks.keys() {
            m.insert(k.clone(), ());
        }
        m.into_keys().collect()
    }

    /// 保留满足谓词的 ID。
    pub fn retain<F>(&mut self, mut pred: F)
    where
        F: FnMut(&str) -> bool,
    {
        self.tasks.retain(|k, _| pred(k));
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn schedule_then_list_contains_id() {
        let mut s = Scheduler::new();
        s.schedule("job-a");
        assert!(s.contains("job-a"));
        assert_eq!(s.len(), 1);
        let ids: HashSet<_> = s.list().into_iter().collect();
        assert_eq!(ids, HashSet::from(["job-a".to_string()]));
    }

    #[test]
    fn cancel_and_idempotent() {
        let mut s = Scheduler::new();
        s.schedule("job-b");
        assert!(s.cancel("job-b"));
        assert!(!s.cancel("job-b"));
        s.schedule("job-c");
        s.schedule("job-c");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn default_empty_debug_clone() {
        let s = Scheduler::default();
        assert!(s.is_empty());
        let _ = format!("{s:?}");
        let _ = s.clone();
    }

    #[test]
    fn checked_normalized_many() {
        let mut s = Scheduler::new();
        assert!(matches!(
            s.schedule_checked(""),
            Err(ScheduleError::EmptyId)
        ));
        s.schedule_checked("ok").unwrap();
        s.schedule_normalized("  t1  ").unwrap();
        s.schedule_many(["a", "b"]);
        assert_eq!(s.cancel_many(["a", "x"]), 1);
        assert!(s.try_schedule("new"));
        assert!(!s.try_schedule("new"));
        assert!(format!("{}", ScheduleError::EmptyId).contains("不能为空"));
        assert!(format!("{}", ScheduleError::IdTooLong { max: MAX_ID_LEN }).contains("最大长度"));
        assert!(format!("{}", ScheduleError::IdControlChar).contains("控制字符"));
        assert!(format!("{}", ScheduleError::JobFailed("x".into())).contains("任务执行失败"));
    }

    #[test]
    fn set_ops_and_retain() {
        let mut a = Scheduler::new();
        a.schedule_many(["1", "2", "3"]);
        let mut b = Scheduler::new();
        b.schedule_many(["2", "4"]);
        assert_eq!(
            a.intersection_ids(&b).into_iter().collect::<HashSet<_>>(),
            HashSet::from(["2".to_string()])
        );
        assert!(a.difference_ids(&b).contains(&"1".to_string()));
        assert_eq!(a.union_ids(&b).len(), 4);
        a.retain(|id| id != "3");
        assert!(!a.contains("3"));
        a.clear();
        assert!(a.is_empty());
    }

    #[test]
    fn bulk_matrix() {
        let mut s = Scheduler::new();
        for i in 0..60 {
            s.schedule(format!("j{i}"));
        }
        assert_eq!(s.len(), 60);
        s.retain(|id| !id.ends_with('0'));
        assert!(s.len() < 60);
        let other = s.clone();
        assert_eq!(s.intersection_ids(&other).len(), s.len());
        let empty: [&str; 0] = [];
        s.schedule_many(empty);
        assert_eq!(s.cancel_many(empty), 0);
    }

    #[test]
    fn job_runner_once_and_fixed_delay() {
        use std::sync::{Arc, Mutex};
        let hits = Arc::new(Mutex::new(0u32));
        let h = Arc::clone(&hits);
        let mut r = JobRunner::new();
        r.add(
            Job::new("t", move || {
                *h.lock().unwrap() += 1;
                Ok(())
            }),
            Schedule::once(5),
        )
        .unwrap();
        assert_eq!(r.tick(4).fired, 0);
        assert_eq!(r.tick(5).fired, 1);
        assert!(Schedule::cron("bad * *").is_err());
        assert!(format!("{}", ScheduleError::InvalidSchedule("x".into())).contains("非法调度"));
    }
}

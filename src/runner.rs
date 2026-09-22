//! 确定性 Job 运行器：`tick(now_ms)` 驱动，无墙钟依赖。

use std::collections::HashMap;

use crate::job::{Job, JobId, JobMeta};
use crate::schedule::{cron_matches, parse_cron_expr, CronParsed, Schedule};
use crate::ScheduleError;

struct Entry {
    job: Job,
    schedule: Schedule,
    /// Once 是否已触发。
    fired_once: bool,
    /// FixedDelay / Cron EveryMs 上次触发时刻。
    last_fire_ms: Option<u64>,
    /// Cron MinuteMatch：上次触发的逻辑分钟索引。
    last_minute_index: Option<u64>,
    cancelled: bool,
}

/// 内存 Job 运行器。
///
/// 调用方注入非递减的 `now_ms`；核心不读系统时间。回退的 tick 被忽略。
/// 同一 tick 内按 `str::cmp` 的 [`JobId`] 字典序执行；单个 Job 的错误或 panic
/// 都不会阻断后续 Job——panic 被捕获并记为该 Job 本次失败。
#[derive(Default)]
pub struct JobRunner {
    entries: HashMap<String, Entry>,
    last_tick_ms: Option<u64>,
}

impl JobRunner {
    /// 空运行器。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 校验并注册 job + schedule；失败不改变 runner，重复 ID 完整覆盖旧状态。
    ///
    /// # Errors
    ///
    /// Job ID 或公开可构造的 [`Schedule`] 状态无效时返回 [`ScheduleError`]。
    pub fn add(&mut self, job: Job, schedule: Schedule) -> Result<(), ScheduleError> {
        crate::validate_task_id(job.id.as_str())?;
        validate_schedule(&schedule)?;
        let id = job.id.as_str().to_string();
        self.entries.insert(
            id,
            Entry {
                job,
                schedule,
                fired_once: false,
                last_fire_ms: None,
                last_minute_index: None,
                cancelled: false,
            },
        );
        Ok(())
    }

    /// 标记取消；返回条目是否存在（重复取消仍返回 `true`，直至 [`Self::remove`]）。
    pub fn cancel(&mut self, id: &str) -> bool {
        if let Some(e) = self.entries.get_mut(id) {
            e.cancelled = true;
            true
        } else {
            false
        }
    }

    /// 移除已取消或任意条目。
    pub fn remove(&mut self, id: &str) -> bool {
        self.entries.remove(id).is_some()
    }

    /// 是否包含（含已取消未移除）。
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    /// 活跃（未取消）数量。
    #[must_use]
    pub fn active_len(&self) -> usize {
        self.entries.values().filter(|e| !e.cancelled).count()
    }

    /// 按 `str::cmp` 的 Job ID 字典序返回元数据列表（包含已取消未移除条目）。
    #[must_use]
    pub fn list_meta(&self) -> Vec<JobMeta> {
        let mut metadata: Vec<_> = self.entries.values().map(|e| e.job.meta()).collect();
        metadata.sort_unstable_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        metadata
    }

    /// 推进逻辑时间：按 `str::cmp` 的 Job ID 字典序运行所有在 `now_ms` 到期的 job。
    ///
    /// 返回成功触发次数；单个 job 错误按执行顺序记入错误列表、推进触发状态，
    /// 其他 job 继续。大跨度 tick 每个 job 最多执行一次，不补跑错过的间隔。
    ///
    /// Job callback panic 会被 `catch_unwind` 捕获，记为该 job 本次失败
    /// （[`ScheduleError::JobPanicked`]）、推进触发状态，同 tick 后续 job 继续执行；
    /// panic 不再中止整轮 tick。默认 panic hook 仍会向 stderr 打印噪声，
    /// 宿主如需静默可自行安装 hook（本 crate 不做全局 hook 副作用）。
    ///
    /// `now_ms` 小于上次 tick 时不执行也不推进。
    pub fn tick(&mut self, now_ms: u64) -> TickResult {
        if self
            .last_tick_ms
            .is_some_and(|last_tick_ms| now_ms < last_tick_ms)
        {
            return TickResult::default();
        }
        self.last_tick_ms = Some(now_ms);
        let mut fired = 0usize;
        let mut errors = Vec::new();
        // 收集到期 ID，避免双重借用
        let mut due: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, e)| !e.cancelled && is_due(e, now_ms))
            .map(|(id, _)| id.clone())
            .collect();
        due.sort_unstable();

        for id in due {
            // 不变量：id 由本函数对 entries 的快照生成，循环内不增删条目，故必然存在。
            // 但 job 回调可能经外部句柄重入增删条目；缺失时跳过并记错，避免 panic 整个调度器。
            let Some(entry) = self.entries.get_mut(&id) else {
                errors.push((
                    JobId::new(id),
                    ScheduleError::JobFailed("due 条目在 tick 内被移除".into()),
                ));
                continue;
            };
            // 隔离第三方 job 的 panic：闭包内只调用回调本身，runner 的状态
            // （entries / last_fire 基线）不在闭包内变更——mark_fired 仅在闭包
            // 正常返回或 panic 被捕获之后执行，因此 job 中途 panic 不会破坏
            // 调度器不变量；这里用 AssertUnwindSafe 是安全的。job 自身捕获的
            // 外部状态在 panic 点可能不一致，那属于宿主回调的责任边界。
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (entry.job.run)()));
            match outcome {
                Ok(Ok(())) => {
                    fired += 1;
                    mark_fired(entry, now_ms);
                }
                Ok(Err(err)) => {
                    // 仍推进 last_fire，避免紧密循环打爆
                    mark_fired(entry, now_ms);
                    errors.push((JobId::new(id), err));
                }
                Err(payload) => {
                    // panic 视为该 job 本次失败：同样推进触发状态，避免一个
                    // 必然 panic 的 job 在每个 tick 反复炸；记入错误通道后继续。
                    mark_fired(entry, now_ms);
                    errors.push((
                        JobId::new(id),
                        ScheduleError::JobPanicked(panic_message(payload.as_ref())),
                    ));
                }
            }
        }
        TickResult { fired, errors }
    }
}

/// 从 panic payload 尽力提取人类可读信息（非字符串 payload 返回占位说明）。
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        return (*text).to_string();
    }
    if let Some(text) = payload.downcast_ref::<String>() {
        return text.clone();
    }
    "非字符串 payload".to_string()
}

fn validate_schedule(schedule: &Schedule) -> Result<(), ScheduleError> {
    match schedule {
        Schedule::Once { .. } => Ok(()),
        Schedule::FixedDelay { every_ms: 0, .. } => Err(ScheduleError::InvalidSchedule(
            "固定间隔 every_ms 必须大于 0".into(),
        )),
        Schedule::FixedDelay { .. } => Ok(()),
        Schedule::Cron { expr, parsed } => {
            let reparsed = parse_cron_expr(expr)?;
            if &reparsed != parsed {
                return Err(ScheduleError::InvalidSchedule(
                    "cron 表达式与解析结果不一致".into(),
                ));
            }
            Ok(())
        }
    }
}

impl std::fmt::Debug for JobRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobRunner")
            .field("jobs", &self.entries.len())
            .field("active", &self.active_len())
            .finish()
    }
}

/// `tick` 结果。
#[derive(Debug, Default)]
pub struct TickResult {
    /// 成功执行次数。
    pub fired: usize,
    /// 按 Job ID 执行顺序排列的失败列表。
    pub errors: Vec<(JobId, ScheduleError)>,
}

fn is_due(entry: &Entry, now_ms: u64) -> bool {
    match &entry.schedule {
        Schedule::Once { at_ms } => !entry.fired_once && now_ms >= *at_ms,
        Schedule::FixedDelay {
            every_ms,
            first_at_ms,
        } => {
            if now_ms < *first_at_ms {
                return false;
            }
            match entry.last_fire_ms {
                None => true,
                Some(last) => now_ms.saturating_sub(last) >= *every_ms,
            }
        }
        Schedule::Cron { parsed, .. } => match parsed {
            CronParsed::EveryMs { every_ms } => match entry.last_fire_ms {
                None => true,
                Some(last) => now_ms.saturating_sub(last) >= *every_ms,
            },
            CronParsed::MinuteMatch { .. } => {
                if !cron_matches(parsed, now_ms) {
                    return false;
                }
                let minute_index = now_ms / 60_000;
                match entry.last_minute_index {
                    None => true,
                    Some(prev) => minute_index > prev,
                }
            }
        },
    }
}

fn mark_fired(entry: &mut Entry, now_ms: u64) {
    entry.last_fire_ms = Some(now_ms);
    entry.last_minute_index = Some(now_ms / 60_000);
    if matches!(entry.schedule, Schedule::Once { .. }) {
        entry.fired_once = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;
    use crate::schedule::Schedule;
    use std::sync::{Arc, Mutex};

    #[test]
    fn once_fires_at_tick() {
        let hits = Arc::new(Mutex::new(0u32));
        let h = Arc::clone(&hits);
        let mut runner = JobRunner::new();
        runner
            .add(
                Job::new("once", move || {
                    *h.lock().unwrap() += 1;
                    Ok(())
                }),
                Schedule::once(100),
            )
            .unwrap();
        assert_eq!(runner.tick(50).fired, 0);
        assert_eq!(runner.tick(100).fired, 1);
        assert_eq!(runner.tick(200).fired, 0);
        assert_eq!(*hits.lock().unwrap(), 1);
    }

    #[test]
    fn fixed_delay_refires() {
        let hits = Arc::new(Mutex::new(0u32));
        let h = Arc::clone(&hits);
        let mut runner = JobRunner::new();
        runner
            .add(
                Job::new("fd", move || {
                    *h.lock().unwrap() += 1;
                    Ok(())
                }),
                Schedule::fixed_delay(10).unwrap(),
            )
            .unwrap();
        assert_eq!(runner.tick(0).fired, 1);
        assert_eq!(runner.tick(5).fired, 0);
        assert_eq!(runner.tick(10).fired, 1);
        assert_eq!(runner.tick(20).fired, 1);
        assert_eq!(*hits.lock().unwrap(), 3);
    }

    #[test]
    fn cancel_stops_job() {
        let mut runner = JobRunner::new();
        runner
            .add(Job::new("c", || Ok(())), Schedule::fixed_delay(1).unwrap())
            .unwrap();
        assert!(runner.cancel("c"));
        assert_eq!(runner.tick(10).fired, 0);
        assert!(runner.contains("c"));
        assert!(runner.remove("c"));
        assert!(!runner.cancel("missing"));
    }

    #[test]
    fn invalid_cron_rejected() {
        assert!(Schedule::cron("not-a-cron").is_err());
        assert!(Schedule::cron("1 2 3 4 5").is_err());
    }

    #[test]
    fn add_rejects_zero_fixed_delay_and_debug_meta() {
        let mut runner = JobRunner::new();
        // FixedDelay every_ms==0 在 add 时拒绝（绕过 Schedule::fixed_delay 校验）
        let bad = Schedule::FixedDelay {
            every_ms: 0,
            first_at_ms: 0,
        };
        assert!(runner.add(Job::new("z", || Ok(())), bad).is_err());
        runner
            .add(Job::new("a", || Ok(())).with_name("n"), Schedule::once(1))
            .unwrap();
        assert_eq!(runner.active_len(), 1);
        assert_eq!(runner.list_meta().len(), 1);
        let _ = format!("{runner:?}");
        assert!(runner.cancel("a"));
        assert_eq!(runner.active_len(), 0);
    }

    #[test]
    fn tick_records_job_errors_and_continues() {
        let mut runner = JobRunner::new();
        runner
            .add(
                Job::new("bad", || Err(ScheduleError::JobFailed("boom".into()))),
                Schedule::once(0),
            )
            .unwrap();
        runner
            .add(Job::new("good", || Ok(())), Schedule::once(0))
            .unwrap();
        let r = runner.tick(0);
        assert_eq!(r.fired, 1);
        assert_eq!(r.errors.len(), 1);
        assert!(format!("{}", r.errors[0].1).contains("任务执行失败"));
    }

    #[test]
    fn panic_job_is_contained_and_does_not_block_same_tick() {
        let mut runner = JobRunner::new();
        runner
            .add(
                Job::new("a-panic", || -> Result<(), ScheduleError> {
                    panic!("boom")
                }),
                Schedule::once(0),
            )
            .unwrap();
        runner
            .add(Job::new("b-ok", || Ok(())), Schedule::once(0))
            .unwrap();
        // panic 被捕获：同 tick 字典序在后的 job 仍执行，panic 记为该 job 失败
        let r = runner.tick(0);
        assert_eq!(r.fired, 1, "b-ok 应成功执行");
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.errors[0].0.as_str(), "a-panic");
        assert!(matches!(&r.errors[0].1, ScheduleError::JobPanicked(msg) if msg == "boom"));
        // 触发状态已推进：Once 不因 panic 重跑
        let r2 = runner.tick(1);
        assert_eq!(r2.fired, 0);
        assert!(r2.errors.is_empty());
    }

    #[test]
    fn panic_message_handles_non_string_payload() {
        assert_eq!(panic_message(&"文本"), "文本");
        assert_eq!(panic_message(&String::from("拥有型文本")), "拥有型文本");
        assert_eq!(panic_message(&42u32), "非字符串 payload");
    }

    #[test]
    fn fixed_delay_respects_first_at() {
        let hits = Arc::new(Mutex::new(0u32));
        let h = Arc::clone(&hits);
        let mut runner = JobRunner::new();
        runner
            .add(
                Job::new("fd", move || {
                    *h.lock().unwrap() += 1;
                    Ok(())
                }),
                Schedule::FixedDelay {
                    every_ms: 10,
                    first_at_ms: 50,
                },
            )
            .unwrap();
        assert_eq!(runner.tick(40).fired, 0);
        assert_eq!(runner.tick(50).fired, 1);
        assert_eq!(runner.tick(55).fired, 0);
        assert_eq!(runner.tick(60).fired, 1);
        assert_eq!(*hits.lock().unwrap(), 2);
    }

    #[test]
    fn cron_every_ms_and_minute_match_paths() {
        let hits = Arc::new(Mutex::new(0u32));
        let h = Arc::clone(&hits);
        let mut runner = JobRunner::new();
        runner
            .add(
                Job::new("every", move || {
                    *h.lock().unwrap() += 1;
                    Ok(())
                }),
                Schedule::cron("every:10").unwrap(),
            )
            .unwrap();
        // 首次：last_fire_ms=None，立即到期；之后按 stateful interval。
        assert_eq!(runner.tick(0).fired, 1);
        assert_eq!(runner.tick(5).fired, 0);
        assert_eq!(runner.tick(10).fired, 1);

        let hits2 = Arc::new(Mutex::new(0u32));
        let h2 = Arc::clone(&hits2);
        let mut minute_runner = JobRunner::new();
        minute_runner
            .add(
                Job::new("min", move || {
                    *h2.lock().unwrap() += 1;
                    Ok(())
                }),
                Schedule::cron("*/5 * * * *").unwrap(),
            )
            .unwrap();
        // minute 0 at t=0
        assert_eq!(minute_runner.tick(0).fired, 1);
        // same minute index → no re-fire for MinuteMatch
        let before = *hits2.lock().unwrap();
        minute_runner.tick(1);
        assert_eq!(*hits2.lock().unwrap(), before);
        // next matching minute (5 min)
        minute_runner.tick(5 * 60_000);
        assert!(*hits2.lock().unwrap() > before);

        // exact minute：不匹配时 is_due 走 cron_matches false 分支
        let mut runner2 = JobRunner::new();
        runner2
            .add(
                Job::new("exact", || Ok(())),
                Schedule::cron("15 * * * *").unwrap(),
            )
            .unwrap();
        assert_eq!(runner2.tick(0).fired, 0);
        assert_eq!(runner2.tick(15 * 60_000).fired, 1);
        // 同一逻辑分钟不再触发
        assert_eq!(runner2.tick(15 * 60_000 + 1).fired, 0);
    }
}

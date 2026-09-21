//! 登记表统计视图。

use crate::Scheduler;

/// 登记表规模快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryStats {
    /// 当前条目数。
    pub len: usize,
    /// 是否为空。
    pub empty: bool,
}

/// 采集统计。
#[must_use]
pub fn stats(s: &Scheduler) -> RegistryStats {
    RegistryStats {
        len: s.len(),
        empty: s.is_empty(),
    }
}

/// 是否「繁忙」（条目数 ≥ 阈值）。
#[must_use]
pub fn is_busy(s: &Scheduler, threshold: usize) -> bool {
    s.len() >= threshold
}

/// 文档用：登记表无硬容量上限（内存 HashMap）。
pub const NO_HARD_CAPACITY: Option<usize> = None;

/// 相对软阈值的利用率（threshold=0 → 0.0）。
#[must_use]
pub fn utilization(s: &Scheduler, soft_threshold: usize) -> f64 {
    if soft_threshold == 0 {
        return 0.0;
    }
    s.len() as f64 / soft_threshold as f64
}

/// 是否超过软阈值。
#[must_use]
pub fn over_soft_threshold(s: &Scheduler, soft_threshold: usize) -> bool {
    soft_threshold > 0 && s.len() > soft_threshold
}

/// 生成人类可读状态行。
#[must_use]
pub fn status_line(s: &Scheduler) -> String {
    format!("registry_len={} empty={}", s.len(), s.is_empty())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::Scheduler;

    #[test]
    fn stats_and_busy() {
        let mut s = Scheduler::new();
        assert_eq!(
            stats(&s),
            RegistryStats {
                len: 0,
                empty: true
            }
        );
        assert!(!is_busy(&s, 1));
        for i in 0..50 {
            s.schedule(format!("t{i}"));
        }
        let st = stats(&s);
        assert_eq!(st.len, 50);
        assert!(!st.empty);
        assert!(is_busy(&s, 50));
        assert!(!is_busy(&s, 51));
        s.clear();
        assert!(stats(&s).empty);
    }

    #[test]
    fn stats_clone_debug() {
        let st = RegistryStats {
            len: 3,
            empty: false,
        };
        let _ = format!("{st:?}");
        assert_eq!(st, st.clone());
    }

    #[test]
    fn utilization_and_status_line() {
        let mut s = Scheduler::new();
        assert_eq!(utilization(&s, 0), 0.0);
        assert!(!over_soft_threshold(&s, 10));
        for i in 0..20 {
            s.schedule(format!("x{i}"));
        }
        assert!(utilization(&s, 10) >= 2.0);
        assert!(over_soft_threshold(&s, 10));
        assert!(status_line(&s).contains("registry_len=20"));
        for i in 0..40 {
            let _ = utilization(&s, 100 + i);
        }
    }
}

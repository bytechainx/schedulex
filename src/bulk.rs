//! 批量登记辅助。

use crate::{validate_task_id, ScheduleError, Scheduler};

/// 批量校验后登记；遇非法 ID 返回错误且不写入该 ID（已写入保留）。
pub fn schedule_checked_many(s: &mut Scheduler, ids: &[&str]) -> Result<usize, ScheduleError> {
    let mut n = 0usize;
    for id in ids {
        validate_task_id(id)?;
        s.schedule(*id);
        n += 1;
    }
    Ok(n)
}

/// 过滤非法 ID 后登记合法者，返回 (ok_count, rejected)。
pub fn schedule_filtering(s: &mut Scheduler, ids: &[&str]) -> (usize, Vec<String>) {
    let mut ok = 0usize;
    let mut bad = Vec::new();
    for id in ids {
        match validate_task_id(id) {
            Ok(()) => {
                s.schedule(*id);
                ok += 1;
            }
            Err(_) => bad.push((*id).to_string()),
        }
    }
    (ok, bad)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Scheduler;

    #[test]
    fn checked_many_and_filter() {
        let mut s = Scheduler::new();
        assert_eq!(schedule_checked_many(&mut s, &["a", "b"]).unwrap(), 2);
        assert!(schedule_checked_many(&mut s, &["", "c"]).is_err());
        let (ok, bad) = schedule_filtering(&mut s, &["ok", "", "x"]);
        assert_eq!(ok, 2);
        assert_eq!(bad, vec!["".to_string()]);
        assert!(s.contains("ok"));
        for i in 0..30 {
            let id = format!("bulk-{i}");
            s.schedule(id);
        }
        assert!(s.len() >= 32);
    }

    #[test]
    fn empty_batch() {
        let mut s = Scheduler::new();
        assert_eq!(schedule_checked_many(&mut s, &[]).unwrap(), 0);
        let (ok, bad) = schedule_filtering(&mut s, &[]);
        assert_eq!(ok, 0);
        assert!(bad.is_empty());
    }

    #[test]
    fn reject_control_in_filter() {
        let mut s = Scheduler::new();
        let (ok, bad) = schedule_filtering(&mut s, &["good", "bad\nid", "also"]);
        assert_eq!(ok, 2);
        assert_eq!(bad.len(), 1);
        assert!(s.contains("good") && s.contains("also"));
    }
}

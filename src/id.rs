//! 任务 ID 校验（登记表门禁，非调度策略）。

use crate::ScheduleError;

/// 最大允许 ID 长度（字节）。
pub const MAX_ID_LEN: usize = 256;

/// 校验任务 ID：非空、不过长、无控制字符。
pub fn validate_task_id(id: &str) -> Result<(), ScheduleError> {
    if id.is_empty() {
        return Err(ScheduleError::EmptyId);
    }
    if id.len() > MAX_ID_LEN {
        return Err(ScheduleError::IdTooLong { max: MAX_ID_LEN });
    }
    if id.chars().any(|c| c.is_control()) {
        return Err(ScheduleError::IdControlChar);
    }
    Ok(())
}

/// 规范化：trim；若结果为空则错误。
///
/// # Examples
///
/// ```
/// use schedulex::normalize_task_id;
///
/// let id = normalize_task_id("  job-1  ")?;
/// assert_eq!(id, "job-1");
/// assert!(normalize_task_id("   ").is_err(), "全空白等价于空 ID");
/// # Ok::<(), schedulex::ScheduleError>(())
/// ```
pub fn normalize_task_id(id: &str) -> Result<String, ScheduleError> {
    let t = id.trim();
    validate_task_id(t)?;
    Ok(t.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_normal() {
        validate_task_id("job-1").unwrap();
        assert_eq!(normalize_task_id("  job-1  ").unwrap(), "job-1");
    }

    #[test]
    fn validate_rejects_bad() {
        assert_eq!(validate_task_id("").unwrap_err(), ScheduleError::EmptyId);
        assert_eq!(
            validate_task_id(&"x".repeat(MAX_ID_LEN + 1)).unwrap_err(),
            ScheduleError::IdTooLong { max: MAX_ID_LEN }
        );
        assert_eq!(
            validate_task_id("a\nb").unwrap_err(),
            ScheduleError::IdControlChar
        );
        assert!(normalize_task_id("   ").is_err());
    }

    #[test]
    fn max_len_boundary() {
        let ok = "a".repeat(MAX_ID_LEN);
        validate_task_id(&ok).unwrap();
        let bad = "a".repeat(MAX_ID_LEN + 1);
        let err = validate_task_id(&bad).unwrap_err();
        assert_eq!(err, ScheduleError::IdTooLong { max: MAX_ID_LEN });
        assert!(format!("{err}").contains(&MAX_ID_LEN.to_string()));
        assert!(!format!("{err}").contains("不能为空"));
        assert_eq!(
            validate_task_id("x\u{0001}y").unwrap_err(),
            ScheduleError::IdControlChar
        );
        assert!(format!("{}", ScheduleError::IdControlChar).contains("控制字符"));
        for id in ["job", "job-1", "ns.task", "A_B"] {
            assert!(validate_task_id(id).is_ok(), "{id}");
            assert_eq!(normalize_task_id(&format!("  {id}  ")).unwrap(), id);
        }
    }

    #[test]
    fn normalize_preserves_inner_spaces_only_trim_ends() {
        let id = normalize_task_id("  name with space  ").unwrap();
        assert_eq!(id, "name with space");
    }

    #[test]
    fn many_valid_ids() {
        for i in 0..80 {
            let id = format!("task-{i}-suffix");
            validate_task_id(&id).unwrap();
            let n = normalize_task_id(&format!(" \t{id}\t ")).unwrap();
            assert_eq!(n, id);
        }
    }
}

/// 生成稳定调试标签（非唯一 ID 生成器；仅拼接前缀）。
#[must_use]
pub fn debug_label(prefix: &str, id: &str) -> String {
    if prefix.is_empty() {
        id.to_string()
    } else {
        format!("{prefix}:{id}")
    }
}

/// 是否为「安全默认」调试标签（含冒号分隔）。
#[must_use]
pub fn is_debug_label(s: &str) -> bool {
    s.contains(':') && validate_task_id(s.split(':').next_back().unwrap_or("")).is_ok()
}

#[cfg(test)]
mod label_tests {
    use super::*;

    #[test]
    fn debug_label_joins() {
        assert_eq!(debug_label("p", "id"), "p:id");
        assert_eq!(debug_label("", "id"), "id");
        for i in 0..25 {
            let l = debug_label("job", &format!("{i}"));
            assert!(l.starts_with("job:"));
            assert!(is_debug_label(&l));
            validate_task_id(&format!("id-{i}")).unwrap();
        }
        assert!(!is_debug_label("nocolon"));
    }
}

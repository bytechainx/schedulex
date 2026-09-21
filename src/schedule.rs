//! 调度表达式（确定性；无墙钟依赖）。
//!
//! # 支持子集
//!
//! | 变体 | 语义 |
//! |------|------|
//! | [`Schedule::Once`] | 在 `at_ms` 时刻触发一次 |
//! | [`Schedule::FixedDelay`] | 首次在 `first_at_ms`（默认 0）后，每 `every_ms` 再触发 |
//! | [`Schedule::Cron`] | **最小子集**：stateful interval `every:<ms>`，或 5 段 `min hour dom mon dow` 中 **仅分钟** 支持 `*` / `*/N` / 单整数；其余字段须为 `*` |
//!
//! 不支持：秒字段、列表/范围、名称月份、时区。

use crate::ScheduleError;

/// 调度策略。
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// 在绝对毫秒时刻触发一次（由 runner 的 `now_ms` 语义定义）。
    Once {
        /// 触发时刻（ms）。
        at_ms: u64,
    },
    /// 固定间隔。
    FixedDelay {
        /// 间隔毫秒（须 > 0）。
        every_ms: u64,
        /// 首次可触发时刻；默认 0 表示从 epoch 起即可。
        first_at_ms: u64,
    },
    /// 最小 cron / every 表达式（见模块文档）。
    Cron {
        /// 原始表达式。
        expr: String,
        /// 解析后的内部形式。
        parsed: CronParsed,
    },
}

/// 解析后的最小 cron。
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronParsed {
    /// 每 `every_ms` 触发；`JobRunner` 首次 tick 立即执行，随后按上次执行时刻计算。
    ///
    /// 公开 [`crate::cron_matches`] 仍是无状态 epoch predicate，不能表达此运行时 interval。
    EveryMs {
        /// 间隔。
        every_ms: u64,
    },
    /// 每分钟逻辑时间对齐的简化模型：当 `now_ms / 60_000` 满足分钟谓词时触发。
    ///
    /// **注意**：这是逻辑分钟索引，不是真实 UTC 墙钟；便于确定性单测。
    MinuteMatch {
        /// `None` = 每分钟；`Some(n)` = 分钟索引 % n == 0（n≥1）；
        /// 单值模式见 `exact`。
        every_n: Option<u32>,
        /// 精确分钟索引（0..=59 映射到逻辑分钟 % 60）；与 `every_n` 互斥优先 `exact`。
        exact: Option<u32>,
    },
}

impl Schedule {
    /// `Once` 便捷构造。
    #[must_use]
    pub const fn once(at_ms: u64) -> Self {
        Self::Once { at_ms }
    }

    /// `FixedDelay` 便捷构造（`first_at_ms = 0`）。
    pub fn fixed_delay(every_ms: u64) -> Result<Self, ScheduleError> {
        if every_ms == 0 {
            return Err(ScheduleError::InvalidSchedule(
                "固定间隔 every_ms 必须大于 0".into(),
            ));
        }
        Ok(Self::FixedDelay {
            every_ms,
            first_at_ms: 0,
        })
    }

    /// 解析 cron / every 表达式。
    ///
    /// 支持：
    /// - `every:1000` / `every:1s`（仅 `ms` 数字或 `Ns` 秒）
    /// - 5 段：`* * * * *`、`*/5 * * * *`、`15 * * * *`（仅分钟段可变，其余必须 `*`）
    pub fn cron(expr: impl Into<String>) -> Result<Self, ScheduleError> {
        let expr = expr.into();
        let parsed = parse_cron_expr(&expr)?;
        Ok(Self::Cron { expr, parsed })
    }
}

/// 解析最小 cron 子集。
pub fn parse_cron_expr(expr: &str) -> Result<CronParsed, ScheduleError> {
    let e = expr.trim();
    if e.is_empty() {
        return Err(ScheduleError::InvalidSchedule("cron 表达式不能为空".into()));
    }
    if let Some(rest) = e.strip_prefix("every:") {
        let rest = rest.trim();
        let every_ms = if let Some(secs) = rest.strip_suffix('s').or_else(|| rest.strip_suffix('S'))
        {
            let n: u64 = secs.trim().parse().map_err(|_| {
                ScheduleError::InvalidSchedule(format!("无法解析 every 秒数: {rest}"))
            })?;
            n.checked_mul(1000)
                .ok_or_else(|| ScheduleError::InvalidSchedule("every 时长毫秒换算溢出".into()))?
        } else {
            rest.parse::<u64>().map_err(|_| {
                ScheduleError::InvalidSchedule(format!("无法解析 every 毫秒数: {rest}"))
            })?
        };
        if every_ms == 0 {
            return Err(ScheduleError::InvalidSchedule("every_ms 必须大于 0".into()));
        }
        return Ok(CronParsed::EveryMs { every_ms });
    }

    let parts: Vec<&str> = e.split_whitespace().collect();
    if parts.len() != 5 {
        return Err(ScheduleError::InvalidSchedule(format!(
            "cron 必须是 5 段或 every:<ms>，实际为 {e:?}"
        )));
    }
    for (i, p) in parts.iter().enumerate().skip(1) {
        if *p != "*" {
            return Err(ScheduleError::InvalidSchedule(format!(
                "仅分钟字段允许非 *；第 {i} 段为 {p:?}"
            )));
        }
    }
    let min = parts[0];
    if min == "*" {
        return Ok(CronParsed::MinuteMatch {
            every_n: None,
            exact: None,
        });
    }
    if let Some(n) = min.strip_prefix("*/") {
        let n: u32 = n
            .parse()
            .map_err(|_| ScheduleError::InvalidSchedule(format!("无法解析分钟步长: {min}")))?;
        if n == 0 {
            return Err(ScheduleError::InvalidSchedule("分钟步长必须大于 0".into()));
        }
        return Ok(CronParsed::MinuteMatch {
            every_n: Some(n),
            exact: None,
        });
    }
    let exact: u32 = min
        .parse()
        .map_err(|_| ScheduleError::InvalidSchedule(format!("无法解析分钟: {min}")))?;
    if exact > 59 {
        return Err(ScheduleError::InvalidSchedule("分钟必须在 0..=59".into()));
    }
    Ok(CronParsed::MinuteMatch {
        every_n: None,
        exact: Some(exact),
    })
}

/// 判断 `now_ms` 是否落在表达式的 epoch 对齐点。
///
/// 这是无状态 predicate；[`crate::JobRunner`] 的 `every:<ms>` 使用 stateful interval，
/// 不调用本函数。MinuteMatch 仍用本函数判断逻辑分钟。
#[must_use]
pub fn cron_matches(parsed: &CronParsed, now_ms: u64) -> bool {
    match parsed {
        CronParsed::EveryMs { every_ms } => {
            if *every_ms == 0 {
                return false;
            }
            now_ms % every_ms == 0
        }
        CronParsed::MinuteMatch { every_n, exact } => {
            let minute_of_hour = (now_ms / 60_000) % 60;
            if let Some(ex) = exact {
                return minute_of_hour == u64::from(*ex);
            }
            match every_n {
                None => true,
                Some(n) if *n > 0 => minute_of_hour % u64::from(*n) == 0,
                Some(_) => false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_every_and_cron() {
        assert!(matches!(
            parse_cron_expr("every:100").unwrap(),
            CronParsed::EveryMs { every_ms: 100 }
        ));
        assert!(matches!(
            parse_cron_expr("every:2s").unwrap(),
            CronParsed::EveryMs { every_ms: 2000 }
        ));
        assert!(parse_cron_expr("every:0").is_err());
        assert!(parse_cron_expr("every:bad").is_err());
        assert!(parse_cron_expr("every:badS").is_err());
        assert!(parse_cron_expr("").is_err());
        assert!(parse_cron_expr("* * *").is_err());
        assert!(parse_cron_expr("1 2 * * *").is_err());
        let star = parse_cron_expr("* * * * *").unwrap();
        assert!(matches!(
            star,
            CronParsed::MinuteMatch {
                every_n: None,
                exact: None
            }
        ));
        let m = parse_cron_expr("*/5 * * * *").unwrap();
        assert!(matches!(
            m,
            CronParsed::MinuteMatch {
                every_n: Some(5),
                exact: None
            }
        ));
        assert!(parse_cron_expr("*/0 * * * *").is_err());
        assert!(parse_cron_expr("*/x * * * *").is_err());
        let ex = parse_cron_expr("15 * * * *").unwrap();
        assert!(matches!(
            ex,
            CronParsed::MinuteMatch {
                exact: Some(15),
                ..
            }
        ));
        assert!(parse_cron_expr("60 * * * *").is_err());
        assert!(parse_cron_expr("xx * * * *").is_err());
        assert!(Schedule::fixed_delay(0).is_err());
        Schedule::fixed_delay(10).unwrap();
        Schedule::cron("every:50").unwrap();
        // every seconds overflow
        assert!(parse_cron_expr(&format!("every:{}s", u64::MAX)).is_err());
    }

    #[test]
    fn cron_match_minute() {
        // minute 0 at t=0
        assert!(cron_matches(
            &CronParsed::MinuteMatch {
                every_n: None,
                exact: None
            },
            0
        ));
        assert!(cron_matches(
            &CronParsed::MinuteMatch {
                every_n: Some(5),
                exact: None
            },
            0
        ));
        // 5 minutes = 300_000 ms → minute_of_hour 5
        assert!(cron_matches(
            &CronParsed::MinuteMatch {
                every_n: Some(5),
                exact: None
            },
            300_000
        ));
        assert!(!cron_matches(
            &CronParsed::MinuteMatch {
                every_n: Some(5),
                exact: None
            },
            60_000
        ));
        assert!(cron_matches(
            &CronParsed::MinuteMatch {
                every_n: None,
                exact: Some(15)
            },
            15 * 60_000
        ));
        assert!(cron_matches(&CronParsed::EveryMs { every_ms: 100 }, 200));
        assert!(!cron_matches(&CronParsed::EveryMs { every_ms: 100 }, 150));
        // every_ms == 0 防御返回 false
        assert!(!cron_matches(&CronParsed::EveryMs { every_ms: 0 }, 0));
        // every_n == 0 防御 false
        assert!(!cron_matches(
            &CronParsed::MinuteMatch {
                every_n: Some(0),
                exact: None
            },
            0
        ));

        // 先在 u64 中取模，避免逻辑分钟超过 u32 后截断并错误命中。
        let beyond_u32_minutes = (u64::from(u32::MAX) + 1) * 60_000;
        assert!(!cron_matches(
            &CronParsed::MinuteMatch {
                every_n: None,
                exact: Some(0)
            },
            beyond_u32_minutes
        ));
        assert!(cron_matches(
            &CronParsed::MinuteMatch {
                every_n: None,
                exact: Some(16)
            },
            beyond_u32_minutes
        ));
    }
}

//! 可执行 Job 抽象（与 ID 登记表 [`crate::Scheduler`] 并存）。

use crate::ScheduleError;

/// Job 标识（与登记表 ID 同校验规则时可复用 `validate_task_id`）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobId(String);

impl JobId {
    /// 构造（不校验；注册到 [`crate::JobRunner`] 时统一校验）。
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// 校验后构造。
    pub fn checked(id: impl Into<String>) -> Result<Self, ScheduleError> {
        let id = id.into();
        crate::validate_task_id(&id)?;
        Ok(Self(id))
    }

    /// 字符串视图。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for JobId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&str> for JobId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for JobId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 同步 Job 函数对象。
pub type JobFn = Box<dyn FnMut() -> Result<(), ScheduleError> + Send>;

/// 已注册的 Job 元数据（不含可调用体时用于查询）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobMeta {
    /// ID。
    pub id: JobId,
    /// 人类可读名称（可选）。
    pub name: Option<String>,
}

/// 可调度 Job：ID + 可调用体。
pub struct Job {
    /// 标识。
    pub id: JobId,
    /// 可选名称。
    pub name: Option<String>,
    /// 执行体。
    pub run: JobFn,
}

impl Job {
    /// 构造。
    pub fn new<F>(id: impl Into<JobId>, run: F) -> Self
    where
        F: FnMut() -> Result<(), ScheduleError> + Send + 'static,
    {
        Self {
            id: id.into(),
            name: None,
            run: Box::new(run),
        }
    }

    /// 带名称。
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// 元数据快照。
    #[must_use]
    pub fn meta(&self) -> JobMeta {
        JobMeta {
            id: self.id.clone(),
            name: self.name.clone(),
        }
    }
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_id_and_run() {
        assert!(JobId::checked("").is_err());
        let id = JobId::checked("j1").unwrap();
        assert_eq!(id.as_str(), "j1");
        assert_eq!(format!("{id}"), "j1");
        assert_eq!(AsRef::<str>::as_ref(&id), "j1");
        let from_string = JobId::from(String::from("s1"));
        assert_eq!(from_string.as_str(), "s1");
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let h = std::sync::Arc::clone(&hits);
        let mut job = Job::new("j1", move || {
            h.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        })
        .with_name("demo");
        (job.run)().unwrap();
        assert_eq!(hits.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(job.meta().name.as_deref(), Some("demo"));
        let _ = format!("{job:?}");
    }
}

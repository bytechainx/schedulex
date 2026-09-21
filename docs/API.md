# schedulex 公开 interface

## Scheduler registry

`Scheduler` 提供登记、取消、查询与集合运算。其 `list`/集合结果顺序不承诺，且不会触发 Job。

## JobRunner tick seam

- `add(Job, Schedule)`：先校验再插入；重复 ID 完整替换旧 callback、schedule 与状态。
- `cancel(id)`：条目存在即返回 true；重复取消仍为 true，直至 `remove`。
- `list_meta()`：包含已取消未移除条目，按 Rust `str::cmp` 的 Job ID 字典序。
- `tick(now_ms)`：到期 Job 按 Rust `str::cmp` 的 Job ID 字典序，每个 tick 每 Job 最多一次。

`now_ms` 为逻辑毫秒。小于上次 tick 的输入被忽略；大跨度 FixedDelay 不补跑。Job Err 进入有序错误列表、推进状态并继续；panic 传播且当前 tick 的部分状态不保证。

## Schedule 子集

- Once
- FixedDelay（`every_ms > 0`）
- `every:<ms>`：首次非回退 tick 立即执行，随后按上次执行时刻计算 interval；跨度不补跑
- 五段 cron 的分钟 `*` / `*/N` / 单整数子集

不支持时区、秒、列表、范围或完整 cron。

`cron_matches()` 是无状态 epoch predicate；`JobRunner` 的 `every:<ms>` interval 不依赖它。五段 Cron 仍按逻辑分钟对齐。

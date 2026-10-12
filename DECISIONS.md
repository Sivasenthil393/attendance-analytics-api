# DECISIONS.md

1. **Indexes.** On `attendance_logs`: unique `(emp_code, date)` to stop duplicates and serve per-employee queries; `(date desc, emp_code)` for the list sort, month ranges and the leaderboard; `(status, date desc, emp_code)` for status filters; `(emp_code, punch_in desc)` to find the record to close at punch-out. On `employees`: unique `emp_code`, `(department, joined_on)` and `joined_on`. I rejected single-field indexes on `status` and `late_minutes`: the compound ones already cover them and extra indexes slow writes.

2. **Punch-in race.** Both requests call `insert_one` with no check first. The unique index lets only one succeed; the other gets `DuplicateKeyError`, which I return as 409.

3. **Ties.** I run `$rank` over all late employees first, then keep `rank <= limit`, so both tied employees appear and the list can be longer than `limit`. Dropping one of them would be arbitrary.

4. **Headcount.** The summary starts from `employees` and `$lookup`s their logs. `preserveNullAndEmptyArrays` keeps people with no logs, and each employee counts as 1.

5. **100x data.** I'd store a pre-computed daily summary per employee, updated on each punch or correction, so analytics read small documents instead of doing a lookup per employee.
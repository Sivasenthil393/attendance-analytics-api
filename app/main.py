"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (a local .env is loaded; real environment variables win)

Layout of this single file
  1. configuration & indexes
  2. time / rounding helpers and the business rules R1-R5
  3. request models and (de)serialisation helpers
  4. employees, attendance (punch-in / punch-out / list / regularize)
  5. analytics (aggregation pipelines) and the explain endpoint
"""
import calendar
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # does not override variables that are already set in the environment
log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration, collections, indexes
# --------------------------------------------------------------------------- #
# tz_aware=True: PyMongo hands back timezone-aware (UTC) datetimes instead of naive ones.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

# Index specs (lists of (field, direction)); the same specs are used as query hints.
IX_LOG_EMP_DATE = [("emp_code", ASCENDING), ("date", ASCENDING)]  # unique
IX_LOG_DATE_EMP = [("date", DESCENDING), ("emp_code", ASCENDING)]
IX_LOG_STATUS_DATE = [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)]
IX_LOG_EMP_PUNCHIN = [("emp_code", ASCENDING), ("punch_in", DESCENDING)]
IX_EMP_CODE = [("emp_code", ASCENDING)]  # unique
IX_EMP_DEPT_JOINED = [("department", ASCENDING), ("joined_on", ASCENDING)]
IX_EMP_JOINED = [("joined_on", ASCENDING)]
IX_EMP_DEPT_CODE = [("department", ASCENDING), ("emp_code", ASCENDING)]

_indexes_ready = False


def ensure_indexes() -> None:
    """Create every index we rely on. Idempotent: re-creating an identical index is a no-op."""
    global _indexes_ready
    specs = [
        (logs, IX_LOG_EMP_DATE, True),
        (logs, IX_LOG_DATE_EMP, False),
        (logs, IX_LOG_STATUS_DATE, False),
        (logs, IX_LOG_EMP_PUNCHIN, False),
        (employees, IX_EMP_CODE, True),
        (employees, IX_EMP_DEPT_JOINED, False),
        (employees, IX_EMP_JOINED, False),
        (employees, IX_EMP_DEPT_CODE, False),
    ]
    ok = True
    for coll, keys, unique in specs:
        try:
            coll.create_index(keys, unique=unique)
        except PyMongoError as exc:  # never crash the app because of one index
            ok = False
            log.error("index %s on %s failed: %s", keys, coll.name, exc)
    _indexes_ready = ok


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except Exception as exc:  # MongoDB may not be up yet; /health retries
        log.error("startup index creation failed: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_, exc: PyMongoError):
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


# --------------------------------------------------------------------------- #
# 2. Helpers: validation errors, time, rounding, business rules R1-R5
# --------------------------------------------------------------------------- #
def invalid(loc: tuple, msg: str):
    """Raise a 422 in FastAPI's standard validation-error format."""
    raise RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": None}])


_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def parse_date(value: str, loc: tuple) -> date:
    """Strict YYYY-MM-DD (rejects 2026-7-1, 2026-02-30, trailing junk)."""
    if not _DATE_RE.fullmatch(value):
        invalid(loc, "date must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        invalid(loc, "not a valid calendar date")


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = int(month[:4]), int(month[5:7])
    if year < 1:
        invalid(("query", "month"), "invalid year")
    last = calendar.monthrange(year, mon)[1]
    return f"{year:04d}-{mon:02d}-01", f"{year:04d}-{mon:02d}-{last:02d}"


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def from_ms(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime, truncated to whole seconds (R1)."""
    return (EPOCH + timedelta(milliseconds=ms)).replace(microsecond=0)


def to_ms(dt: Optional[datetime]) -> Optional[int]:
    """Datetime -> epoch milliseconds using integer maths only (no float drift)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    delta = dt - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def rhu(value, places: int) -> float:
    """Round half-up (not banker's, not binary-float) to `places` decimals."""
    d = value if isinstance(value, Decimal) else Decimal(repr(float(value)))
    return float(d.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def hm(value: str) -> time:
    h, m = value.split(":")
    return time(int(h), int(m))


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return hm(shift_end) <= hm(shift_start)


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: IST calendar date of the punch-in; an overnight shift's early-morning punch-in
    (earlier than shift_end) belongs to the shift that started the previous day."""
    ist = punch_in.astimezone(IST)
    d = ist.date()
    if is_overnight(shift_start, shift_end) and ist.time() < hm(shift_end):
        d -= timedelta(days=1)
    return d.isoformat()


def shift_start_dt(date_str: str, shift_start: str) -> datetime:
    return datetime.combine(date.fromisoformat(date_str), hm(shift_start), tzinfo=IST)


def shift_end_dt(date_str: str, shift_start: str, shift_end: str) -> datetime:
    d = date.fromisoformat(date_str)
    if is_overnight(shift_start, shift_end):
        d += timedelta(days=1)  # the end of an overnight shift is on the next calendar day
    return datetime.combine(d, hm(shift_end), tzinfo=IST)


def compute_late_minutes(punch_in: datetime, date_str: str, shift_start: str) -> int:
    """R2: late only if strictly more than 10:00 after shift_start; then floor(minutes since start)."""
    seconds = int((punch_in - shift_start_dt(date_str, shift_start)).total_seconds())
    return seconds // 60 if seconds > 600 else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600 rounded half-up to 2 decimals."""
    seconds = int((punch_out - punch_in).total_seconds())
    return float((Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out: datetime, date_str: str, shift_start: str, shift_end: str) -> int:
    """R3: floor(minutes after shift_end), counted only when >= 30."""
    seconds = int((punch_out - shift_end_dt(date_str, shift_start, shift_end)).total_seconds())
    minutes = seconds // 60
    return minutes if minutes >= 30 else 0


def derive(emp: dict, date_str: str, status: str, punch_in: Optional[datetime], punch_out: Optional[datetime]) -> dict:
    """All stored derived values for a record, per the table in DATA_MODEL.md."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, date_str, emp["shift_start"])
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    wh = compute_work_hours(punch_in, punch_out)
    return {
        "work_hours": wh,
        "late_minutes": late,
        "overtime_minutes": compute_overtime(punch_out, date_str, emp["shift_start"], emp["shift_end"]),
        "half_day": wh < 4.5,  # R5: on the ROUNDED value
    }


# --------------------------------------------------------------------------- #
# 3. Models and serialisation
# --------------------------------------------------------------------------- #
# Strict int: floats, strings and bools are rejected; the range rejects "seconds" values.
EpochMs = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]
HHMM = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM)
    shift_end: str = Field(default="18:30", pattern=HHMM)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _joined_on(cls, v: str) -> str:
        if not _DATE_RE.fullmatch(v):
            raise ValueError("joined_on must be YYYY-MM-DD")
        date.fromisoformat(v)  # ValueError -> 422
        return v

    @model_validator(mode="after")
    def _shift(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


def jsonable(value: Any) -> Any:
    """Datetimes -> epoch ms, recursively (history.changes nests them)."""
    if isinstance(value, datetime):
        return to_ms(value)
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    return value


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": to_ms(doc.get("created_at")),
    }


def record_out(doc: dict) -> dict:
    """Legacy documents may lack late/overtime/half_day/history: missing means 0 / false / []."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": to_ms(doc.get("punch_in")),
        "punch_out": to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day") or False),
        "history": jsonable(doc.get("history") or []),
    }


def get_employee(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 4a. System + employees
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        return JSONResponse(status_code=503, content={"detail": "mongodb unavailable"})
    if not _indexes_ready:
        ensure_indexes()  # startup ran before Mongo was reachable
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        employees.insert_one(doc)  # the unique index on emp_code is the real guard
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    flt = {"department": department} if department else {}
    total = employees.count_documents(flt)
    cur = employees.find(flt).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cur], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 4b. Attendance
# --------------------------------------------------------------------------- #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee(body.emp_code)
    ts = from_ms(body.punched_at) if body.punched_at is not None else now_utc()
    d = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": d,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(ts, d, emp["shift_start"]),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        logs.insert_one(doc)  # unique (emp_code, date): of N simultaneous requests exactly one wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee(body.emp_code)
    ts = from_ms(body.punched_at) if body.punched_at is not None else now_utc()
    # Most recent record whose punch_in is at or before the punch-out (works across midnight).
    rec = logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}},
        sort=[("punch_in", DESCENDING)],
        hint=IX_LOG_EMP_PUNCHIN,
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    pin = rec["punch_in"]
    if ts <= pin:
        invalid(("body", "punched_at"), "punched_at must be after punch_in")
    if ts - pin > timedelta(hours=24):
        invalid(("body", "punched_at"), "punched_at is more than 24 hours after punch_in")
    fields = derive(emp, rec["date"], rec["status"], pin, ts)
    fields["punch_out"] = ts
    # Atomic compare-and-set: only succeeds if the record is still open and unchanged.
    updated = logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None, "punch_in": pin},
        {"$set": fields},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


def build_list_plan(emp_code, date_from, date_to, status, page, page_size) -> dict:
    """Filter, sort, paging and index hint of GET /attendance (shared with the explain endpoint)."""
    d_from = parse_date(date_from, ("query", "date_from")) if date_from else None
    d_to = parse_date(date_to, ("query", "date_to")) if date_to else None
    if d_from and d_to and d_from > d_to:
        invalid(("query", "date_from"), "date_from must not be after date_to")
    flt: dict = {}
    if emp_code:
        flt["emp_code"] = emp_code
    if d_from or d_to:
        flt["date"] = {}
        if d_from:
            flt["date"]["$gte"] = date_from
        if d_to:
            flt["date"]["$lte"] = date_to
    if status:
        flt["status"] = status
    if emp_code:
        hint = IX_LOG_EMP_DATE  # walked backwards: date DESC for one employee
    elif status:
        hint = IX_LOG_STATUS_DATE
    else:
        hint = IX_LOG_DATE_EMP
    return {
        "collection": logs.name,
        "filter": flt,
        "sort": [("date", DESCENDING), ("emp_code", ASCENDING)],
        "skip": (page - 1) * page_size,
        "limit": page_size,
        "hint": hint,
    }


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    plan = build_list_plan(emp_code, date_from, date_to, status, page, page_size)
    total = logs.count_documents(plan["filter"])
    cur = logs.find(plan["filter"], sort=plan["sort"], skip=plan["skip"], limit=plan["limit"], hint=plan["hint"])
    return {"items": [record_out(d) for d in cur], "total": total, "page": page, "page_size": page_size}


def _plan_regularization(emp: dict, rec: dict, body: RegularizeIn, in_req, out_req, date_str: str):
    """Pure function: returns (fields_to_set, history_entry) or raises 422."""
    old_status = rec["status"]
    old_in, old_out = rec.get("punch_in"), rec.get("punch_out")
    status = body.status or old_status

    if status in ("ABSENT", "LEAVE"):
        if in_req is not None or out_req is not None:
            invalid(("body",), "ABSENT / LEAVE clear the punch times; do not supply punch_in or punch_out")
        new_in = new_out = None
    else:
        new_in = in_req or old_in
        new_out = out_req or old_out
        if new_in is None:
            invalid(("body", "punch_in"), "a presence status requires a punch_in")
        if in_req is not None and attendance_date(new_in, emp["shift_start"], emp["shift_end"]) != date_str:
            invalid(("body", "punch_in"), "punch_in must stay on the record's attendance date")
        if new_out is not None:
            if new_out <= new_in:
                invalid(("body", "punch_out"), "punch_out must be after punch_in")
            if new_out - new_in > timedelta(hours=24):
                invalid(("body", "punch_out"), "punch_out must be within 24 hours of punch_in")

    if status == old_status and to_ms(new_in) == to_ms(old_in) and to_ms(new_out) == to_ms(old_out):
        invalid(("body",), "the request changes nothing")

    new_vals = {"status": status, "punch_in": new_in, "punch_out": new_out}
    new_vals.update(derive(emp, date_str, status, new_in, new_out))
    old_vals = {
        "status": old_status,
        "punch_in": old_in,
        "punch_out": old_out,
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day") or False),
    }
    changes = {}
    for key, new in new_vals.items():
        old = old_vals[key]
        same = to_ms(new) == to_ms(old) if isinstance(new, datetime) or isinstance(old, datetime) else new == old
        if not same:
            changes[key] = {"from": old, "to": new}
    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    return new_vals, entry


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    parse_date(date, ("path", "date"))
    if body.status in ("ABSENT", "LEAVE") and (body.punch_in is not None or body.punch_out is not None):
        invalid(("body",), "ABSENT / LEAVE clear the punch times; do not supply punch_in or punch_out")
    emp = get_employee(emp_code)
    in_req = from_ms(body.punch_in) if body.punch_in is not None else None
    out_req = from_ms(body.punch_out) if body.punch_out is not None else None

    for _ in range(5):  # optimistic concurrency: retry if someone else changed the record meanwhile
        rec = logs.find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")
        new_vals, entry = _plan_regularization(emp, rec, body, in_req, out_req, date)
        guard = {
            "_id": rec["_id"],
            "status": rec["status"],
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(rec.get("history") or [])]},
        }
        updated = logs.find_one_and_update(
            guard,
            {"$set": new_vals, "$push": {"history": entry}},  # $push appends, never overwrites
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            return record_out(updated)
    raise HTTPException(409, "record was modified concurrently; please retry")


# --------------------------------------------------------------------------- #
# 5. Analytics: pipeline builders (each returns collection, pipeline, hint)
# --------------------------------------------------------------------------- #
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"

# Reusable aggregation expressions over a log document.
_IS_PRESENT = {"$in": ["$status", PRESENCE]}
_DOW = {"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}  # 1=Sun .. 7=Sat
_IS_WEEKDAY = {"$in": [_DOW, [2, 3, 4, 5, 6]]}
_HALF = {"$eq": [{"$ifNull": ["$half_day", False]}, True]}
_LATE_MIN = {"$ifNull": ["$late_minutes", 0]}
# Present value in HALF days (2 = full day, 1 = half day): integers keep the maths exact.
_PRESENT_HALVES_ANY_DAY = {"$cond": [_IS_PRESENT, {"$cond": [_HALF, 1, 2]}, 0]}
_PRESENT_HALVES_WEEKDAY = {"$cond": [{"$and": [_IS_PRESENT, _IS_WEEKDAY]}, {"$cond": [_HALF, 1, 2]}, 0]}
_IS_LATE = {"$cond": [{"$gt": [_LATE_MIN, 0]}, 1, 0]}
_HAS_WH = {"$and": [_IS_PRESENT, {"$isNumber": "$work_hours"}]}


def plan_employee_monthly(emp_code: str, month: str) -> dict:
    first, last = month_bounds(month)
    pipeline = [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first, "$lte": last}}},
        {
            "$group": {
                "_id": None,
                "present_halves": {"$sum": _PRESENT_HALVES_WEEKDAY},
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": _IS_LATE},
                "total_late_minutes": {"$sum": _LATE_MIN},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]
    return {"collection": logs.name, "pipeline": pipeline, "hint": IX_LOG_EMP_DATE}


def plan_department_summary(month: str, department: Optional[str]) -> dict:
    first, last = month_bounds(month)
    match: dict = {"joined_on": {"$lte": last}}  # R9: joined on or before month end
    if department:
        match["department"] = department
    per_employee = [  # runs inside $lookup, once per employee, on the (emp_code, date) index
        {"$match": {"date": {"$gte": first, "$lte": last}}},
        {
            "$group": {
                "_id": None,
                "ph": {"$sum": _PRESENT_HALVES_WEEKDAY},
                "wh_cents": {
                    "$sum": {"$cond": [_HAS_WH, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]}
                },
                "wh_n": {"$sum": {"$cond": [_HAS_WH, 1, 0]}},
                "late_count": {"$sum": _IS_LATE},
                "late_total": {"$sum": _LATE_MIN},
                "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
            }
        },
    ]

    def total(field: str) -> dict:
        return {"$sum": {"$ifNull": [f"$s.{field}", 0]}}

    pipeline = [
        {"$match": match},
        {
            "$lookup": {
                "from": logs.name,
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": per_employee,
                "as": "s",
            }
        },
        # Employees with no logs get an empty array; keep them so they still count in headcount.
        {"$unwind": {"path": "$s", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "ph": total("ph"),
                "wh_cents": total("wh_cents"),
                "wh_n": total("wh_n"),
                "late_count": total("late_count"),
                "late_total": total("late_total"),
                "leave_count": total("leave_count"),
                "on_duty_count": total("on_duty_count"),
            }
        },
        {"$sort": {"_id": 1}},
    ]
    hint = IX_EMP_DEPT_JOINED if department else IX_EMP_JOINED
    return {"collection": employees.name, "pipeline": pipeline, "hint": hint}


def plan_late_leaderboard(month: str, limit: int, department: Optional[str]) -> dict:
    first, last = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first, "$lte": last}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": employees.name, "localField": "_id", "foreignField": "emp_code", "as": "e"}},
        {"$unwind": "$e"},  # logs of unknown employees are dropped
    ]
    if department:
        pipeline.append({"$match": {"e.department": department}})  # rank inside the department only
    pipeline += [
        # Standard competition ranking (1, 2, 2, 4) over ALL candidates, BEFORE the limit.
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$e.name",
                "department": "$e.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return {"collection": logs.name, "pipeline": pipeline, "hint": IX_LOG_DATE_EMP}


def plan_department_trend(department: str, d_from: str, d_to: str) -> dict:
    lo = datetime.combine(date.fromisoformat(d_from), time(0), tzinfo=UTC)
    hi = datetime.combine(date.fromisoformat(d_to), time(0), tzinfo=UTC)
    # Every employee emits events: one "hire" event (headcount +1 on the day they count from) and
    # one event per log in range. Events are summed per day, gaps are filled with $densify, and
    # window functions give the running headcount and the 7-day moving average.
    log_events = [
        {"$match": {"date": {"$gte": d_from, "$lte": d_to}}},
        {"$project": {"_id": 0, "date": 1, "ph": _PRESENT_HALVES_ANY_DAY, "late": _IS_LATE}},
    ]
    pipeline = [
        {"$match": {"department": department}},
        {
            "$lookup": {
                "from": logs.name,
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": log_events,
                "as": "logs",
            }
        },
        {
            "$project": {
                "_id": 0,
                "ev": {
                    "$concatArrays": [
                        # a zero event on `from` guarantees the stream is never empty (for $densify)
                        [{"date": d_from, "hc": 0, "ph": 0, "late": 0}],
                        {
                            "$cond": [
                                {"$lte": ["$joined_on", d_to]},
                                [
                                    {
                                        "date": {"$cond": [{"$lt": ["$joined_on", d_from]}, d_from, "$joined_on"]},
                                        "hc": 1,
                                        "ph": 0,
                                        "late": 0,
                                    }
                                ],
                                [],
                            ]
                        },
                        {
                            "$map": {
                                "input": "$logs",
                                "as": "l",
                                "in": {"date": "$$l.date", "hc": 0, "ph": "$$l.ph", "late": "$$l.late"},
                            }
                        },
                    ]
                },
            }
        },
        {"$unwind": "$ev"},
        {"$replaceRoot": {"newRoot": "$ev"}},
        {"$group": {"_id": "$date", "hc": {"$sum": "$hc"}, "ph": {"$sum": "$ph"}, "late": {"$sum": "$late"}}},
        {"$addFields": {"d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        {"$sort": {"d": 1}},
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [lo, hi + timedelta(days=1)]}}},
        {"$match": {"d": {"$lte": hi}}},  # upper bound of $densify is exclusive; this is a safety net
        {"$sort": {"d": 1}},
        {
            "$setWindowFields": {
                "sortBy": {"d": 1},
                "output": {
                    "headcount": {"$sum": {"$ifNull": ["$hc", 0]}, "window": {"documents": ["unbounded", "current"]}}
                },
            }
        },
        {
            "$addFields": {
                "ph": {"$ifNull": ["$ph", 0]},
                "late": {"$ifNull": ["$late", 0]},
                "is_working_day": {"$and": [{"$gte": [{"$dayOfWeek": "$d"}, 2]}, {"$lte": [{"$dayOfWeek": "$d"}, 6]}]},
            }
        },
        {
            "$addFields": {
                "rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {"$divide": [{"$divide": ["$ph", 2]}, "$headcount"]},
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"d": 1},
                "output": {"ma": {"$avg": "$rate", "window": {"documents": [-6, 0]}}},  # nulls are ignored
            }
        },
        {"$project": {"_id": 0, "d": 1, "is_working_day": 1, "headcount": 1, "ph": 1, "late": 1, "ma": 1}},
    ]
    return {"collection": employees.name, "pipeline": pipeline, "hint": IX_EMP_DEPT_JOINED}


def run_pipeline(plan: dict) -> list:
    coll = db[plan["collection"]]
    return list(coll.aggregate(plan["pipeline"], hint=dict(plan["hint"])))


# --------------------------------------------------------------------------- #
# 5b. Analytics endpoints
# --------------------------------------------------------------------------- #
def count_weekdays(start: date, end: date) -> int:
    n, d = 0, start
    while d <= end:
        if d.weekday() < 5:
            n += 1
        d += timedelta(days=1)
    return n


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=MONTH_PATTERN)):
    emp = get_employee(emp_code)
    first, last = month_bounds(month)
    start = max(date.fromisoformat(first), date.fromisoformat(emp["joined_on"]))  # R7
    working_days = count_weekdays(start, date.fromisoformat(last))
    res = run_pipeline(plan_employee_monthly(emp_code, month))
    g = res[0] if res else {}
    halves = int(g.get("present_halves", 0))
    pct = None
    if working_days > 0:
        pct = rhu(Decimal(halves) * 100 / (Decimal(2) * working_days), 2)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": rhu(Decimal(halves) / 2, 2),
        "leave_days": int(g.get("leave_days", 0)),
        "late_count": int(g.get("late_count", 0)),
        "total_late_minutes": int(g.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(g.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=MONTH_PATTERN), department: Optional[str] = None):
    items = []
    for g in run_pipeline(plan_department_summary(month, department)):
        n = int(g["wh_n"])
        avg = None
        if n > 0:  # mean over RECORDS: sum of centi-hours / count, exact decimal, half-up
            avg = rhu(Decimal(int(g["wh_cents"])) / Decimal(n) / Decimal(100), 2)
        items.append(
            {
                "department": g["_id"],
                "headcount": int(g["headcount"]),
                "present_days": rhu(Decimal(int(g["ph"])) / 2, 2),
                "avg_work_hours": avg,
                "late_count": int(g["late_count"]),
                "total_late_minutes": int(g["late_total"]),
                "leave_count": int(g["leave_count"]),
                "on_duty_count": int(g["on_duty_count"]),
            }
        )
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=MONTH_PATTERN),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    return {"month": month, "items": run_pipeline(plan_late_leaderboard(month, limit, department))}


def _validate_trend_range(d_from: str, d_to: str) -> None:
    a = parse_date(d_from, ("query", "from"))
    b = parse_date(d_to, ("query", "to"))
    if b < a:
        invalid(("query", "to"), "to must not be before from")
    if (b - a).days + 1 > 92:
        invalid(("query", "to"), "range must not exceed 92 days")


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, d_from: str = Query(..., alias="from"), d_to: str = Query(..., alias="to")):
    _validate_trend_range(d_from, d_to)
    if employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "unknown department")
    items = []
    for r in run_pipeline(plan_department_trend(department, d_from, d_to)):
        hc, ph = int(r["headcount"]), int(r["ph"])
        rate = None
        if r["is_working_day"] and hc > 0:
            rate = rhu(Decimal(ph) / Decimal(2 * hc), 4)  # exact integer ratio, half-up
        items.append(
            {
                "date": r["d"].strftime("%Y-%m-%d"),
                "is_working_day": bool(r["is_working_day"]),
                "headcount": hc,
                "present_count": ph / 2,
                "late_count": int(r["late"]),
                "attendance_rate": rate,
                "moving_avg_7d": None if r.get("ma") is None else rhu(r["ma"], 4),
            }
        )
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 5c. Explain
# --------------------------------------------------------------------------- #
def _clean(doc: Any) -> Any:
    """BSON -> plain JSON (dates / ObjectIds become Extended JSON)."""
    return json.loads(json_util.dumps(doc))


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_PATTERN),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    d_from: Optional[str] = Query(None, alias="from"),
    d_to: Optional[str] = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    def need(**values):
        for name, val in values.items():
            if val is None or val == "":
                invalid(("query", name), f"{name} is required for {endpoint}")

    if endpoint == "attendance_list":
        plan = build_list_plan(emp_code, date_from, date_to, status, page, page_size)
        cmd = {
            "find": plan["collection"],
            "filter": plan["filter"],
            "sort": dict(plan["sort"]),
            "skip": plan["skip"],
            "limit": plan["limit"],
            "hint": dict(plan["hint"]),
        }
    else:
        if endpoint == "employee_monthly":
            need(emp_code=emp_code, month=month)
            plan = plan_employee_monthly(emp_code, month)
        elif endpoint == "department_summary":
            need(month=month)
            plan = plan_department_summary(month, department)
        elif endpoint == "late_leaderboard":
            need(month=month)
            plan = plan_late_leaderboard(month, limit, department)
        else:  # department_trend
            need(department=department, **{"from": d_from, "to": d_to})
            _validate_trend_range(d_from, d_to)
            plan = plan_department_trend(department, d_from, d_to)
        cmd = {"aggregate": plan["collection"], "pipeline": plan["pipeline"], "cursor": {}, "hint": dict(plan["hint"])}
    result = db.command({"explain": cmd, "verbosity": "executionStats"})
    return {"endpoint": endpoint, "collection": plan["collection"], "explain": _clean(result)}

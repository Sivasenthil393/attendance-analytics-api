# Employee Attendance & Analytics API

Employee attendance tracking (punch-in / punch-out with business rules, manual corrections with an audit trail) and analytics built on MongoDB aggregation pipelines. Python 3.11+, FastAPI, MongoDB 6.0+.

## Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="attendance_db"
uvicorn app.main:app --port 8000
```

MongoDB can be a local container (`docker run -p 27017:27017 mongo:7`) or a free Atlas cluster. Indexes are created automatically at startup. Interactive docs: http://localhost:8000/docs

## Notes

- All code is in `app/main.py`. Review findings are in `REVIEW.md`, design answers in `DECISIONS.md`.
- API instants are epoch milliseconds; MongoDB stores them as UTC dates. Calendar dates and shift times are IST strings.
- Analytics read the stored derived values and run as aggregation pipelines (`$lookup`, `$setWindowFields` with `$rank`, `$densify`).
- `/admin/explain/{endpoint}` returns the real MongoDB query plan for each endpoint.
from fastapi import FastAPI, Depends, HTTPException, Query, UploadFile, File, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse, JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, case
from sqlalchemy.exc import OperationalError
from typing import Optional, List
import calendar
from datetime import date as dt_date
import os
import hmac
from pathlib import Path
import inspect
from urllib.parse import quote
import models, schemas
from database import engine, get_db
from import_excel import (
    import_from_excel,
    import_from_excel_bytes,
    import_employees_only_from_excel_bytes,
    import_permissions_from_excel_bytes,
    import_leaves_from_excel_bytes,
)

# Create tables (new columns added)
models.Base.metadata.create_all(bind=engine)
def _ensure_employee_allowance_columns() -> None:
    with engine.begin() as conn:
        cols = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(employees)").fetchall()}
        if "leave_allowance_annual_days" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN leave_allowance_annual_days REAL DEFAULT 0.0")
        if "leave_allowance_casual_days" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN leave_allowance_casual_days REAL DEFAULT 0.0")
        if "contract_end_date" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN contract_end_date TEXT")
        if "national_id" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN national_id TEXT")
        if "insurance_number" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN insurance_number TEXT")
        if "education" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN education TEXT")
        if "university" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN university TEXT")
        if "marital_status" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN marital_status TEXT")
        if "religion" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN religion TEXT")
        if "governorate" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN governorate TEXT")
        if "city" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN city TEXT")
        if "area" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN area TEXT")
        if "address" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN address TEXT")
        if "iban" not in cols:
            conn.exec_driver_sql("ALTER TABLE employees ADD COLUMN iban TEXT")

_ensure_employee_allowance_columns()

app = FastAPI(title="HR Management System", version="2.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

def _render_template(request: Request, name: str, context: dict):
    ctx = dict(context or {})
    ctx.setdefault("request", request)
    try:
        sig = inspect.signature(templates.TemplateResponse)
        if "request" in sig.parameters:
            return templates.TemplateResponse(request, name, ctx)
    except Exception:
        pass
    return templates.TemplateResponse(name, ctx)

@app.get("/debug/info", include_in_schema=False)
def debug_info():
    with engine.connect() as conn:
        cols = [row[1] for row in conn.exec_driver_sql("PRAGMA table_info(employees)").fetchall()]
    employee_out_fields = list(getattr(schemas.EmployeeOut, "model_fields", getattr(schemas.EmployeeOut, "__fields__", {})).keys())
    summary_out_fields = list(getattr(schemas.EmployeeSummaryOut, "model_fields", getattr(schemas.EmployeeSummaryOut, "__fields__", {})).keys())
    return {
        "app_version": getattr(app, "version", None),
        "engine_url": str(getattr(engine, "url", "")),
        "employees_table_has_leave_allowance_annual_days": "leave_allowance_annual_days" in cols,
        "employees_table_has_leave_allowance_casual_days": "leave_allowance_casual_days" in cols,
        "employee_out_has_leave_allowance_annual_days": "leave_allowance_annual_days" in employee_out_fields,
        "employee_out_has_leave_allowance_casual_days": "leave_allowance_casual_days" in employee_out_fields,
        "summary_out_has_leave_allowance_annual_days": "leave_allowance_annual_days" in summary_out_fields,
        "summary_out_has_leave_allowance_casual_days": "leave_allowance_casual_days" in summary_out_fields,
    }

def _safe_next_url(next_url: Optional[str]) -> str:
    if not next_url:
        return "/dashboard"
    if not next_url.startswith("/"):
        return "/dashboard"
    if next_url.startswith("//") or "://" in next_url:
        return "/dashboard"
    return next_url

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if (
        path.startswith("/static")
        or path in {"/login", "/logout", "/openapi.json", "/docs", "/redoc"}
    ):
        return await call_next(request)

    if request.session.get("auth") is True:
        return await call_next(request)

    html_exact = {"/", "/dashboard", "/daily", "/departments-page", "/employees-page", "/upload"}
    is_html = path in html_exact or path.startswith("/department/") or path.startswith("/employee-page/")
    if is_html:
        next_url = request.url.path
        if request.url.query:
            next_url = f"{next_url}?{request.url.query}"
        return RedirectResponse(url=f"/login?next={quote(next_url, safe='')}", status_code=302)

    return JSONResponse({"detail": "Not authenticated"}, status_code=401)

app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("HR_SECRET_KEY", "dev-secret-key"),
    same_site="lax",
)

@app.get("/login", include_in_schema=False)
def login_page(request: Request, next: Optional[str] = None, error: Optional[str] = None):
    if request.session.get("auth") is True:
        return RedirectResponse(url="/dashboard", status_code=302)
    return _render_template(request, "login.html", {"next": _safe_next_url(next), "error": error})

@app.post("/login", include_in_schema=False)
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: Optional[str] = Form(None),
):
    env_user = os.environ.get("HR_USERNAME", "admin")
    env_pass = os.environ.get("HR_PASSWORD", "HR@2025")
    ok = hmac.compare_digest(username, env_user) and hmac.compare_digest(password, env_pass)
    if not ok:
        return RedirectResponse(
            url=f"/login?error=1&next={quote(_safe_next_url(next), safe='')}",
            status_code=302,
        )
    request.session["auth"] = True
    return RedirectResponse(url=_safe_next_url(next), status_code=302)

@app.get("/logout", include_in_schema=False)
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)


# ── Month Overview ─────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
def month_overview(request: Request):
    today = dt_date.today()
    year_month = f"{today.year:04d}-{today.month:02d}"
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    return _render_template(request, "month.html", {"year_month": year_month, "days_in_month": days_in_month})


@app.get("/latest-date")
def latest_date(db: Session = Depends(get_db)):
    latest = db.query(func.max(models.AttendanceRecord.date)).scalar()
    return {"date": str(latest) if latest else None}


@app.get("/calendar-status")
def calendar_status(
    month: Optional[str] = Query(None, description="Month in YYYY-MM"),
    db: Session = Depends(get_db),
):
    ym = month
    if not ym:
        today = dt_date.today()
        ym = f"{today.year:04d}-{today.month:02d}"
    rows = (
        db.query(func.distinct(models.AttendanceRecord.date))
        .filter(models.AttendanceRecord.date.like(f"{ym}%"))
        .all()
    )
    dates = sorted([r[0] for r in rows if r[0]])
    return {"month": ym, "dates": dates}


# ── Upload ─────────────────────────────────────────────────────────────────────
@app.get("/upload", include_in_schema=False)
def upload_page(request: Request):
    return _render_template(request, "upload.html", {})


# ── Dashboard ──────────────────────────────────────────────────────────────────
@app.get("/dashboard", include_in_schema=False)
def dashboard(request: Request):
    return _render_template(request, "index.html", {})


@app.get("/daily", include_in_schema=False)
def daily(request: Request):
    return _render_template(request, "index.html", {})


# ── Stats ──────────────────────────────────────────────────────────────────────
@app.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    q_filter = ~models.Employee.dept.like("%vistor%")

    total       = db.query(func.count(models.Employee.id)).filter(q_filter).scalar()
    active      = db.query(func.count(models.Employee.id)).filter(q_filter, models.Employee.status == "active").scalar()
    avg_rests   = db.query(func.avg(models.Employee.rests)).filter(q_filter).scalar() or 0
    dept_count  = db.query(func.count(func.distinct(models.Employee.dept))).filter(q_filter).scalar()

    dept_breakdown = (
        db.query(models.Employee.dept, func.count(models.Employee.id).label("count"))
        .filter(q_filter)
        .group_by(models.Employee.dept)
        .all()
    )

    return {
        "total_employees": total,
        "active_employees": active,
        "avg_leave_days": round(float(avg_rests), 1),
        "department_count": dept_count,
        "dept_breakdown": [{"dept": d, "count": c} for d, c in dept_breakdown],
    }


# ── Departments ────────────────────────────────────────────────────────────────
@app.get("/departments", response_model=List[str])
def get_departments(db: Session = Depends(get_db)):
    try:
        rows = db.query(models.Department.name).order_by(models.Department.name.asc()).all()
        names = [r[0] for r in rows if r[0]]
        if not names:
            emp_rows = db.query(func.distinct(models.Employee.dept)).all()
            emp_names = [r[0] for r in emp_rows if r[0]]
            existing = set()
            for n in emp_names:
                nn = str(n).strip()
                if nn and nn not in existing:
                    db.add(models.Department(name=nn))
                    existing.add(nn)
            if existing:
                db.commit()
            rows = db.query(models.Department.name).order_by(models.Department.name.asc()).all()
            names = [r[0] for r in rows if r[0]]
        if names:
            return names
    except Exception:
        pass
    rows = db.query(func.distinct(models.Employee.dept)).all()
    return sorted([r[0] for r in rows if r[0]])


# ── Employees ──────────────────────────────────────────────────────────────────
@app.get("/employees", response_model=List[schemas.EmployeeOut])
def list_employees(
    search: Optional[str] = Query(None, description="Search by name or job title"),
    dept:   Optional[str] = Query(None, description="Filter by department"),
    status: Optional[str] = Query(None, description="Filter by status"),
    ids:    Optional[str] = Query(None, description="Filter by employee ids (comma-separated)"),
    db:     Session = Depends(get_db),
):
    q = db.query(models.Employee)
    if ids:
        parsed: list[int] = []
        for part in str(ids).replace(";", ",").split(","):
            p = part.strip()
            if not p:
                continue
            try:
                parsed.append(int(p))
            except Exception:
                continue
        if parsed:
            q = q.filter(models.Employee.id.in_(parsed))
    if search:
        term = f"%{search}%"
        q = q.filter(
            models.Employee.name.ilike(term) |
            models.Employee.job_title.ilike(term) |
            models.Employee.phone.ilike(term) |
            models.Employee.email.ilike(term) |
            models.Employee.employee_code.ilike(term)
        )
    if dept:
        q = q.filter(models.Employee.dept == dept)
    if status:
        q = q.filter(models.Employee.status == status)
    return q.order_by(models.Employee.id).all()


@app.post("/employees", response_model=schemas.EmployeeOut, status_code=201)
def create_employee(payload: schemas.EmployeeCreate, db: Session = Depends(get_db)):
    emp = models.Employee(**payload.model_dump())
    db.add(emp)
    dept_name = (emp.dept or "").strip()
    if dept_name:
        try:
            dep = db.query(models.Department).filter(models.Department.name == dept_name).first()
            if dep is None:
                db.add(models.Department(name=dept_name))
        except Exception:
            pass
    db.commit()
    db.refresh(emp)
    return emp


@app.get("/employees/{emp_id}", response_model=schemas.EmployeeOut)
def get_employee(emp_id: int, db: Session = Depends(get_db)):
    try:
        emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    except OperationalError as e:
        msg = str(e).lower()
        if "no such column" in msg and "leave_allowance" in msg:
            _ensure_employee_allowance_columns()
            emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
        else:
            raise
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    return emp


@app.put("/employees/{emp_id}", response_model=schemas.EmployeeOut)
def update_employee(emp_id: int, payload: schemas.EmployeeUpdate, db: Session = Depends(get_db)):
    emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(emp, field, value)
    try:
        db.commit()
    except OperationalError as e:
        db.rollback()
        msg = str(e).lower()
        if "no such column" in msg and "leave_allowance" in msg:
            _ensure_employee_allowance_columns()
            db.commit()
        else:
            raise
    db.refresh(emp)
    return emp


@app.delete("/employees/{emp_id}", status_code=204)
def delete_employee(emp_id: int, db: Session = Depends(get_db)):
    emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    db.delete(emp)
    db.commit()


# ── Import Excel ───────────────────────────────────────────────────────────────
@app.post("/import/excel", response_model=schemas.ImportExcelResult)
def import_excel(payload: schemas.ImportExcelRequest, db: Session = Depends(get_db)):
    _ = db
    res = import_from_excel(file=payload.file, reset_db=payload.reset_db, allow_create_employees=False)
    return schemas.ImportExcelResult(
        file=res["file"],
        employees_upserted=res["employees_upserted"],
        attendance_rows_upserted=res["attendance_rows_upserted"],
        distinct_dates=res["distinct_dates"],
    )


@app.post("/import/upload", response_model=schemas.ImportExcelResult)
async def import_upload(
    file: UploadFile = File(...),
    reset_db: bool = Form(False),
    db: Session = Depends(get_db),
):
    _ = db
    content = await file.read()
    res = import_from_excel_bytes(content, file_name=file.filename or "upload.xlsx", reset_db=reset_db, allow_create_employees=False)
    return schemas.ImportExcelResult(
        file=res["file"],
        employees_upserted=res["employees_upserted"],
        attendance_rows_upserted=res["attendance_rows_upserted"],
        distinct_dates=res["distinct_dates"],
    )


@app.post("/import/employees/upload")
async def import_employees_upload(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _ = db
    content = await file.read()
    try:
        return import_employees_only_from_excel_bytes(content, file_name=file.filename or "employees.xlsx")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"تعذر استيراد ملف الموظفين: {e}")


@app.post("/import/permissions/upload")
async def import_permissions_upload(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _ = db
    content = await file.read()
    return import_permissions_from_excel_bytes(content, file_name=file.filename or "permissions.xlsx")


@app.post("/import/leaves/upload")
async def import_leaves_upload(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _ = db
    content = await file.read()
    return import_leaves_from_excel_bytes(content, file_name=file.filename or "leaves.xlsx")


# ── Attendance ────────────────────────────────────────────────────────────────
@app.get("/attendance", response_model=List[schemas.AttendanceRowOut])
def list_attendance(
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    employee_code: Optional[str] = Query(None, description="Filter by employee code"),
    db: Session = Depends(get_db),
):
    q = db.query(
        models.AttendanceRecord,
        models.Employee.employee_code,
        models.Employee.name,
        models.Employee.dept,
    ).join(models.Employee)
    if date:
        q = q.filter(models.AttendanceRecord.date == date)
    if employee_code:
        q = q.filter(models.Employee.employee_code == employee_code)
    rows = q.order_by(models.AttendanceRecord.date.desc(), models.AttendanceRecord.id.desc()).all()
    return [
        schemas.AttendanceRowOut(
            employee_code=code,
            name=name,
            dept=dept,
            date=rec.date,
            actual_check_in_time=rec.actual_check_in_time,
            actual_check_out_time=rec.actual_check_out_time,
            attendance_records=rec.attendance_records,
            total_work_hours=rec.total_work_hours,
        )
        for rec, code, name, dept in rows
    ]


@app.get("/leaves", response_model=List[schemas.LeaveRowOut])
def list_leaves(
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    employee_code: Optional[str] = Query(None, description="Filter by employee code"),
    db: Session = Depends(get_db),
):
    q = db.query(
        models.AttendanceRecord,
        models.Employee.employee_code,
        models.Employee.name,
        models.Employee.dept,
    ).join(models.Employee)
    if date:
        q = q.filter(models.AttendanceRecord.date == date)
    if employee_code:
        q = q.filter(models.Employee.employee_code == employee_code)
    rows = q.order_by(models.AttendanceRecord.date.desc(), models.AttendanceRecord.id.desc()).all()
    return [
        schemas.LeaveRowOut(
            employee_code=code,
            name=name,
            dept=dept,
            date=rec.date,
            annual_leave=float(rec.annual_leave or 0.0),
            personal_leave=float(rec.personal_leave or 0.0),
        )
        for rec, code, name, dept in rows
        if (rec.annual_leave or 0) != 0 or (rec.personal_leave or 0) != 0
    ]


@app.get("/monthly-stats", response_model=List[schemas.MonthlyStatsOut])
def list_monthly_stats(
    month: Optional[str] = Query(None, description="Month in YYYY-MM"),
    db: Session = Depends(get_db),
):
    ym = month
    if not ym:
        latest = db.query(func.max(models.AttendanceRecord.date)).scalar()
        if latest:
            ym = str(latest)[:7]
    if not ym:
        return []

    rows = (
        db.query(models.MonthlyEmployeeStats, models.Employee.employee_code, models.Employee.name, models.Employee.dept)
        .join(models.Employee, models.MonthlyEmployeeStats.employee_id == models.Employee.id)
        .filter(models.MonthlyEmployeeStats.year_month == ym)
        .order_by(models.Employee.employee_code.asc())
        .all()
    )
    return [
        schemas.MonthlyStatsOut(
            employee_code=code,
            name=name,
            dept=dept,
            year_month=ms.year_month,
            total_work_minutes=int(ms.total_work_minutes or 0),
            days_present=int(ms.days_present or 0),
            days_absent=int(ms.days_absent or 0),
            annual_leave_days=float(ms.annual_leave_days or 0.0),
            casual_leave_days=float(ms.casual_leave_days or 0.0),
            ot_total=float(ms.ot_total or 0.0),
        )
        for ms, code, name, dept in rows
    ]


@app.get("/mtd-stats", response_model=List[schemas.MtdStatsOut])
def month_to_date_stats(
    date: Optional[str] = Query(None, description="To date (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    to_date = date
    if not to_date:
        latest = db.query(func.max(models.AttendanceRecord.date)).scalar()
        if latest:
            to_date = str(latest)
    if not to_date or len(to_date) < 10:
        return []

    ym = to_date[:7]
    rows = (
        db.query(
            models.Employee.employee_code,
            models.Employee.name,
            models.Employee.dept,
            func.coalesce(func.sum(case((models.AttendanceRecord.attendance_status == "A", 1), else_=0)), 0).label("days_absent"),
            func.coalesce(func.sum(case((models.AttendanceRecord.attendance_status != "A", 1), else_=0)), 0).label("days_present"),
            func.coalesce(func.sum(case((models.AttendanceRecord.attendance_status.in_(["L", "LE"]), 1), else_=0)), 0).label("delays"),
            func.coalesce(func.sum(case((models.AttendanceRecord.attendance_status.in_(["E", "LE"]), 1), else_=0)), 0).label("permissions"),
            func.coalesce(func.sum(func.coalesce(models.AttendanceRecord.annual_leave, 0.0)), 0.0).label("annual_leave_days"),
            func.coalesce(func.sum(func.coalesce(models.AttendanceRecord.personal_leave, 0.0)), 0.0).label("casual_leave_days"),
        )
        .join(models.Employee, models.AttendanceRecord.employee_id == models.Employee.id)
        .filter(models.AttendanceRecord.date.like(f"{ym}%"))
        .filter(models.AttendanceRecord.date <= to_date)
        .group_by(models.Employee.employee_code, models.Employee.name, models.Employee.dept)
        .order_by(models.Employee.employee_code.asc())
        .all()
    )

    return [
        schemas.MtdStatsOut(
            employee_code=code,
            name=name,
            dept=dept,
            year_month=ym,
            to_date=to_date,
            days_present=int(days_present or 0),
            days_absent=int(days_absent or 0),
            delays=int(delays or 0),
            permissions=int(permissions or 0),
            annual_leave_days=float(annual_leave_days or 0.0),
            casual_leave_days=float(casual_leave_days or 0.0),
        )
        for (
            code,
            name,
            dept,
            days_absent,
            days_present,
            delays,
            permissions,
            annual_leave_days,
            casual_leave_days,
        ) in rows
    ]


@app.get("/employees/{emp_id}/attendance", response_model=List[schemas.AttendanceRecordOut])
def employee_attendance(emp_id: int, db: Session = Depends(get_db)):
    emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    return (
        db.query(models.AttendanceRecord)
        .filter(models.AttendanceRecord.employee_id == emp_id)
        .order_by(models.AttendanceRecord.date.desc(), models.AttendanceRecord.id.desc())
        .all()
    )


# ── Department Pages ───────────────────────────────────────────────────────────
@app.get("/departments-page", include_in_schema=False)
def departments_page(request: Request):
    return _render_template(request, "departments.html", {})


@app.get("/department/{dept_name}", include_in_schema=False)
def department_page(dept_name: str, request: Request):
    return _render_template(request, "department.html", {"dept_name": dept_name})


@app.get("/employee-page/{emp_id}", include_in_schema=False)
def employee_page(emp_id: int, request: Request):
    return _render_template(request, "employee.html", {"emp_id": emp_id})


@app.get("/employees-page", include_in_schema=False)
def employees_page(request: Request):
    return _render_template(request, "employees.html", {})


# ── Employee Summary ───────────────────────────────────────────────────────────
@app.get("/employees/{emp_id}/summary", response_model=schemas.EmployeeSummaryOut)
def employee_summary(emp_id: int, db: Session = Depends(get_db)):
    try:
        emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    except OperationalError as e:
        msg = str(e).lower()
        if "no such column" in msg and "leave_allowance" in msg:
            _ensure_employee_allowance_columns()
            emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
        else:
            raise
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    total_annual  = sum(l.days  for l in emp.leaves if l.leave_type == "annual")
    total_casual  = sum(l.days  for l in emp.leaves if l.leave_type == "casual")
    total_hours   = sum(p.hours for p in emp.permissions)
    total_deduct_d  = sum(d.amount for d in emp.deductions if d.deduction_type == "days")
    total_deduct_m  = sum(d.amount for d in emp.deductions if d.deduction_type == "money")
    allowance_annual = float(getattr(emp, "leave_allowance_annual_days", 0.0) or 0.0)
    allowance_casual = float(getattr(emp, "leave_allowance_casual_days", 0.0) or 0.0)
    annual_used = float(total_annual or 0.0)
    casual_used = float(total_casual or 0.0)
    annual_remaining = max(0.0, allowance_annual - annual_used)
    casual_remaining = max(0.0, allowance_casual - casual_used)
    total_used = annual_used + casual_used
    remaining_before_deduct = annual_remaining + casual_remaining
    remaining = max(0.0, remaining_before_deduct - float(total_deduct_d or 0.0))

    return schemas.EmployeeSummaryOut(
        id=emp.id,
        employee_code=emp.employee_code,
        name=emp.name,
        dept=emp.dept,
        job_title=emp.job_title,
        status=emp.status,
        leave_allowance_annual_days=allowance_annual,
        leave_allowance_casual_days=allowance_casual,
        total_annual_leave=total_annual,
        total_casual_leave=total_casual,
        total_leave_used_days=total_used,
        total_permission_hours=total_hours,
        total_deduction_days=total_deduct_d,
        total_deduction_money=total_deduct_m,
        annual_leave_remaining_days=annual_remaining,
        casual_leave_remaining_days=casual_remaining,
        leave_remaining_days=remaining,
    )


# ── Department Employees Summary ───────────────────────────────────────────────
@app.get("/departments/{dept_name}/employees", response_model=List[schemas.EmployeeSummaryOut])
def dept_employees(dept_name: str, db: Session = Depends(get_db)):
    employees = (
        db.query(models.Employee)
        .filter(models.Employee.dept == dept_name)
        .order_by(models.Employee.name)
        .all()
    )
    result = []
    for emp in employees:
        total_annual = sum(l.days  for l in emp.leaves if l.leave_type == "annual")
        total_casual = sum(l.days  for l in emp.leaves if l.leave_type == "casual")
        total_hours  = sum(p.hours for p in emp.permissions)
        total_deduct_d  = sum(d.amount for d in emp.deductions if d.deduction_type == "days")
        total_deduct_m  = sum(d.amount for d in emp.deductions if d.deduction_type == "money")
        allowance_annual = float(getattr(emp, "leave_allowance_annual_days", 0.0) or 0.0)
        allowance_casual = float(getattr(emp, "leave_allowance_casual_days", 0.0) or 0.0)
        annual_used = float(total_annual or 0.0)
        casual_used = float(total_casual or 0.0)
        annual_remaining = max(0.0, allowance_annual - annual_used)
        casual_remaining = max(0.0, allowance_casual - casual_used)
        total_used = annual_used + casual_used
        remaining_before_deduct = annual_remaining + casual_remaining
        remaining = max(0.0, remaining_before_deduct - float(total_deduct_d or 0.0))
        result.append(schemas.EmployeeSummaryOut(
            id=emp.id,
            employee_code=emp.employee_code,
            name=emp.name,
            dept=emp.dept,
            job_title=emp.job_title,
            status=emp.status,
            leave_allowance_annual_days=allowance_annual,
            leave_allowance_casual_days=allowance_casual,
            total_annual_leave=total_annual,
            total_casual_leave=total_casual,
            total_leave_used_days=total_used,
            total_permission_hours=total_hours,
            total_deduction_days=total_deduct_d,
            total_deduction_money=total_deduct_m,
            annual_leave_remaining_days=annual_remaining,
            casual_leave_remaining_days=casual_remaining,
            leave_remaining_days=remaining,
        ))
    return result


# ── Manual Leaves ──────────────────────────────────────────────────────────────
@app.post("/employees/{emp_id}/leaves", response_model=schemas.LeaveOut, status_code=201)
def add_leave(emp_id: int, payload: schemas.LeaveCreate, db: Session = Depends(get_db)):
    _ensure_employee_allowance_columns()
    emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")

    leave_type = (payload.leave_type or "").strip().lower()
    if leave_type not in {"annual", "casual"}:
        raise HTTPException(status_code=400, detail="نوع الإجازة غير صحيح (annual/casual)")

    days = float(payload.days or 0.0)
    if days <= 0:
        raise HTTPException(status_code=400, detail="عدد الأيام يجب أن يكون أكبر من صفر")

    total_annual = float(sum(l.days for l in emp.leaves if l.leave_type == "annual") or 0.0)
    total_casual = float(sum(l.days for l in emp.leaves if l.leave_type == "casual") or 0.0)
    total_deduct_d = float(sum(d.amount for d in emp.deductions if d.deduction_type == "days") or 0.0)

    allowance_annual = float(getattr(emp, "leave_allowance_annual_days", 0.0) or 0.0)
    allowance_casual = float(getattr(emp, "leave_allowance_casual_days", 0.0) or 0.0)

    annual_remaining = max(0.0, allowance_annual - total_annual)
    casual_remaining = max(0.0, allowance_casual - total_casual)
    total_remaining = max(0.0, (annual_remaining + casual_remaining) - total_deduct_d)

    if leave_type == "annual" and days > annual_remaining:
        raise HTTPException(
            status_code=400,
            detail=f"لا يوجد رصيد سنوي كافي. المتبقي: {annual_remaining:g} يوم",
        )
    if leave_type == "casual" and days > casual_remaining:
        raise HTTPException(
            status_code=400,
            detail=f"لا يوجد رصيد عارضة كافي. المتبقي: {casual_remaining:g} يوم",
        )
    if days > total_remaining:
        raise HTTPException(
            status_code=400,
            detail=f"لا يوجد رصيد إجازات كافي بعد الخصم. المتبقي: {total_remaining:g} يوم",
        )

    from datetime import datetime as _dt
    leave = models.Leave(
        employee_id=emp_id,
        leave_type=leave_type,
        days=days,
        date=payload.date,
        note=payload.note or "",
        created_at=_dt.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    db.add(leave)
    db.commit()
    db.refresh(leave)
    return leave


@app.get("/employees/{emp_id}/leaves", response_model=List[schemas.LeaveOut])
def list_employee_leaves(emp_id: int, db: Session = Depends(get_db)):
    emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    return db.query(models.Leave).filter(models.Leave.employee_id == emp_id).order_by(models.Leave.date.desc()).all()


@app.delete("/employees/{emp_id}/leaves/{leave_id}", status_code=204)
def delete_leave(emp_id: int, leave_id: int, db: Session = Depends(get_db)):
    leave = db.query(models.Leave).filter(models.Leave.id == leave_id, models.Leave.employee_id == emp_id).first()
    if not leave:
        raise HTTPException(status_code=404, detail="Leave not found")
    db.delete(leave)
    db.commit()


# ── Manual Permissions ─────────────────────────────────────────────────────────
@app.post("/employees/{emp_id}/permissions", response_model=schemas.PermissionOut, status_code=201)
def add_permission(emp_id: int, payload: schemas.PermissionCreate, db: Session = Depends(get_db)):
    emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    ym = (payload.date or "")[:7]
    if len(ym) != 7 or ym[4] != "-":
        raise HTTPException(status_code=400, detail="صيغة التاريخ غير صحيحة (YYYY-MM-DD)")
    month_total = (
        db.query(func.coalesce(func.sum(models.Permission.hours), 0.0))
        .filter(models.Permission.employee_id == emp_id)
        .filter(models.Permission.date.like(f"{ym}%"))
        .scalar()
    )
    month_total = float(month_total or 0.0)
    hours = float(payload.hours or 0.0)
    if hours <= 0:
        raise HTTPException(status_code=400, detail="عدد الساعات يجب أن يكون أكبر من صفر")
    if month_total + hours > 4:
        remaining = max(0.0, 4.0 - month_total)
        r_txt = str(int(remaining)) if remaining % 1 == 0 else f"{remaining:.1f}"
        t_txt = str(int(month_total)) if month_total % 1 == 0 else f"{month_total:.1f}"
        raise HTTPException(
            status_code=400,
            detail=f"لا يمكن إضافة الإذن: تم استخدام {t_txt} ساعة هذا الشهر، والمتبقي {r_txt} ساعة فقط (الحد الأقصى 4 ساعات).",
        )
    from datetime import datetime as _dt
    perm = models.Permission(
        employee_id=emp_id,
        hours=payload.hours,
        date=payload.date,
        note=payload.note or "",
        created_at=_dt.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    db.add(perm)
    db.commit()
    db.refresh(perm)
    return perm


@app.get("/employees/{emp_id}/permissions", response_model=List[schemas.PermissionOut])
def list_employee_permissions(emp_id: int, db: Session = Depends(get_db)):
    emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    return db.query(models.Permission).filter(models.Permission.employee_id == emp_id).order_by(models.Permission.date.desc()).all()


@app.delete("/employees/{emp_id}/permissions/{perm_id}", status_code=204)
def delete_permission(emp_id: int, perm_id: int, db: Session = Depends(get_db)):
    perm = db.query(models.Permission).filter(models.Permission.id == perm_id, models.Permission.employee_id == emp_id).first()
    if not perm:
        raise HTTPException(status_code=404, detail="Permission not found")
    db.delete(perm)
    db.commit()


# ── Manual Deductions ──────────────────────────────────────────────────────────
@app.post("/employees/{emp_id}/deductions", response_model=schemas.DeductionOut, status_code=201)
def add_deduction(emp_id: int, payload: schemas.DeductionCreate, db: Session = Depends(get_db)):
    emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    from datetime import datetime as _dt
    ded = models.Deduction(
        employee_id=emp_id,
        deduction_type="days",
        amount=payload.amount,
        reason=payload.reason or "",
        date=payload.date,
        created_at=_dt.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    db.add(ded)
    db.commit()
    db.refresh(ded)
    return ded


@app.get("/employees/{emp_id}/deductions", response_model=List[schemas.DeductionOut])
def list_employee_deductions(emp_id: int, db: Session = Depends(get_db)):
    emp = db.query(models.Employee).filter(models.Employee.id == emp_id).first()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    return db.query(models.Deduction).filter(models.Deduction.employee_id == emp_id).order_by(models.Deduction.date.desc()).all()


@app.delete("/employees/{emp_id}/deductions/{ded_id}", status_code=204)
def delete_deduction(emp_id: int, ded_id: int, db: Session = Depends(get_db)):
    ded = db.query(models.Deduction).filter(models.Deduction.id == ded_id, models.Deduction.employee_id == emp_id).first()
    if not ded:
        raise HTTPException(status_code=404, detail="Deduction not found")
    db.delete(ded)
    db.commit()

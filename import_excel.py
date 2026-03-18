import pandas as pd
import re
import io
from typing import Optional, Tuple, Dict, Any, List
from sqlalchemy import func, case
from database import engine
from models import Base, Employee, AttendanceRecord, MonthlyEmployeeStats
from sqlalchemy.orm import Session

DEFAULT_EXCEL_FILE = "test0-hik.xlsx"

def _normalize_col(c: Any) -> str:
    return str(c).strip().lower().replace(" ", "_")

def _none_if_blank(v: Any):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        if s in {"", "-", "nan"}:
            return None
        return s
    return v

def _to_float(v: Any) -> Optional[float]:
    v = _none_if_blank(v)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None

def _parse_work_minutes(s: Any) -> Optional[int]:
    s = _none_if_blank(s)
    if s is None:
        return None
    txt = str(s)
    m = re.search(r"(\d+)\s*:\s*(\d+)", txt)
    if not m:
        return None
    h = int(m.group(1))
    mm = int(m.group(2))
    return h * 60 + mm

def _detect_header_row_for_test0_hik(file: str, max_scan_rows: int = 40) -> Optional[int]:
    preview = pd.read_excel(file, header=None, nrows=max_scan_rows)
    need = {"first name", "id", "department", "date", "week"}
    best: Tuple[int, int] | None = None
    for i in range(len(preview)):
        row = {_normalize_col(c).replace("_", " ") for c in preview.iloc[i].tolist() if _none_if_blank(c) is not None}
        score = len(need & row)
        if score >= 4 and (best is None or score > best[1]):
            best = (i, score)
    return best[0] if best else None

def _detect_header_row_for_test0_hik_preview(preview: pd.DataFrame) -> Optional[int]:
    need = {"first name", "id", "department", "date", "week"}
    best: Tuple[int, int] | None = None
    for i in range(len(preview)):
        row = {_normalize_col(c).replace("_", " ") for c in preview.iloc[i].tolist() if _none_if_blank(c) is not None}
        score = len(need & row)
        if score >= 4 and (best is None or score > best[1]):
            best = (i, score)
    return best[0] if best else None

def _read_test0_hik(file: str) -> pd.DataFrame:
    header_row = _detect_header_row_for_test0_hik(file)
    if header_row is None:
        raise ValueError("Could not detect header row for 'test0-hik' format.")
    df = pd.read_excel(file, header=header_row)
    df.columns = [_normalize_col(c) for c in df.columns]
    return df

def _read_test0_hik_bytes(content: bytes, max_scan_rows: int = 40) -> pd.DataFrame:
    preview = pd.read_excel(io.BytesIO(content), header=None, nrows=max_scan_rows)
    header_row = _detect_header_row_for_test0_hik_preview(preview)
    if header_row is None:
        raise ValueError("Could not detect header row for 'test0-hik' format.")
    df = pd.read_excel(io.BytesIO(content), header=header_row)
    df.columns = [_normalize_col(c) for c in df.columns]
    return df

def _read_employee_list(file: str) -> pd.DataFrame:
    df = pd.read_excel(file)
    df.columns = [_normalize_col(c) for c in df.columns]
    column_map = {
        "id":        "id",
        "employee_code": "employee_code",
        "name":      "name",
        "dept":      "dept",
        "job_title": "job_title",
        "phone":     "phone",
        "email":     "email",
        "salary":    "salary",
        "join_date": "join_date",
        "rests":     "rests",
        "status":    "status",
    }
    df = df.rename(columns=column_map)
    known = list(column_map.values())
    return df[[c for c in known if c in df.columns]]

def import_from_excel(file: str = DEFAULT_EXCEL_FILE, reset_db: bool = True) -> Dict[str, Any]:
    try:
        df_preview = pd.read_excel(file, header=None, nrows=20)
    except FileNotFoundError:
        raise FileNotFoundError(f"'{file}' not found. Place it in the same directory.") from None

    detected_test0_hik = False
    header_row = None
    try:
        header_row = _detect_header_row_for_test0_hik(file)
        detected_test0_hik = header_row is not None
    except Exception:
        detected_test0_hik = False

    if reset_db:
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
    else:
        Base.metadata.create_all(bind=engine)

    employees_upserted = 0
    attendance_rows_upserted = 0
    distinct_dates: List[str] = []

    with Session(engine) as db:
        if detected_test0_hik:
            df = _read_test0_hik(file)
            distinct_dates = sorted({str(d) for d in df.get("date", pd.Series(dtype=str)).dropna().unique().tolist()})

            for _, row in df.iterrows():
                code = _none_if_blank(row.get("id"))
                name = _none_if_blank(row.get("first_name"))
                dept = _none_if_blank(row.get("department")) or ""
                date = _none_if_blank(row.get("date"))
                if code is None or name is None or date is None:
                    continue

                emp = db.query(Employee).filter(Employee.employee_code == str(code)).first()
                if emp is None:
                    emp = Employee(employee_code=str(code), name=str(name), dept=str(dept), job_title="Employee")
                    db.add(emp)
                    db.flush()
                    employees_upserted += 1
                else:
                    changed = False
                    if emp.name != str(name):
                        emp.name = str(name)
                        changed = True
                    if dept and emp.dept != str(dept):
                        emp.dept = str(dept)
                        changed = True
                    if emp.job_title is None:
                        emp.job_title = "Employee"
                        changed = True
                    if changed:
                        employees_upserted += 1

                rec = db.query(AttendanceRecord).filter(
                    AttendanceRecord.employee_id == emp.id,
                    AttendanceRecord.date == str(date),
                ).first()

                payload = {
                    "week": _none_if_blank(row.get("week")),
                    "actual_check_in_time": _none_if_blank(row.get("actual_check-in_time")) or _none_if_blank(row.get("actual_check_in_time")),
                    "actual_check_out_time": _none_if_blank(row.get("actual_check-out_time")) or _none_if_blank(row.get("actual_check_out_time")),
                    "attendance_records": _none_if_blank(row.get("attendance_records")),
                    "total_work_hours": _none_if_blank(row.get("total_work_hours")),
                    "total_work_minutes": _parse_work_minutes(row.get("total_work_hours")),
                    "attendance_status": _none_if_blank(row.get("attendance_status")),
                    "ot1": _to_float(row.get("ot1")),
                    "ot2": _to_float(row.get("ot2")),
                    "ot3": _to_float(row.get("ot3")),
                    "sick_leave": _to_float(row.get("sick_leave")),
                    "maternity_leave": _to_float(row.get("maternity_leave")),
                    "annual_leave": _to_float(row.get("annual_leave")),
                    "personal_leave": _to_float(row.get("personal_leave")),
                    "paternity_leave": _to_float(row.get("paternity_leave")),
                    "parental_leave": _to_float(row.get("parental_leave")),
                    "family_reunion_leave": _to_float(row.get("family_reunion_leave")),
                    "bereavement_leave": _to_float(row.get("bereavement_leave")),
                    "business_trip": _to_float(row.get("business_trip")),
                    "overtime_exchange_holiday": _to_float(row.get("overtime_exchange_holiday")),
                    "business_trip_exchange_holiday": _to_float(row.get("business_trip_exchange_holiday")),
                }

                if rec is None:
                    rec = AttendanceRecord(employee_id=emp.id, date=str(date), **payload)
                    db.add(rec)
                    attendance_rows_upserted += 1
                else:
                    for k, v in payload.items():
                        setattr(rec, k, v)
                    attendance_rows_upserted += 1

            _recompute_monthly_stats(db, _extract_year_months(distinct_dates))
        else:
            df = _read_employee_list(file)
            for _, row in df.iterrows():
                data = {k: (None if pd.isna(v) else v) for k, v in row.items()}
                code = _none_if_blank(data.get("employee_code"))
                if code is not None:
                    emp = db.query(Employee).filter(Employee.employee_code == str(code)).first()
                    if emp is None:
                        emp = Employee(**{k: data.get(k) for k in data.keys()})
                        emp.employee_code = str(code)
                        db.add(emp)
                        employees_upserted += 1
                    else:
                        for k, v in data.items():
                            if k == "employee_code":
                                continue
                            if v is not None:
                                setattr(emp, k, v)
                        employees_upserted += 1
                else:
                    emp = Employee(**{k: data.get(k) for k in data.keys()})
                    db.add(emp)
                    employees_upserted += 1

        db.commit()

    return {
        "file": file,
        "employees_upserted": employees_upserted,
        "attendance_rows_upserted": attendance_rows_upserted,
        "distinct_dates": distinct_dates,
        "detected_format": "test0-hik" if detected_test0_hik else "employee-list",
    }

def import_from_excel_bytes(content: bytes, file_name: str = "upload.xlsx", reset_db: bool = False) -> Dict[str, Any]:
    detected_test0_hik = False
    try:
        preview = pd.read_excel(io.BytesIO(content), header=None, nrows=40)
        detected_test0_hik = _detect_header_row_for_test0_hik_preview(preview) is not None
    except Exception:
        detected_test0_hik = False

    if reset_db:
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
    else:
        Base.metadata.create_all(bind=engine)

    employees_upserted = 0
    attendance_rows_upserted = 0
    distinct_dates: List[str] = []

    with Session(engine) as db:
        if detected_test0_hik:
            df = _read_test0_hik_bytes(content)
            distinct_dates = sorted({str(d) for d in df.get("date", pd.Series(dtype=str)).dropna().unique().tolist()})

            for _, row in df.iterrows():
                code = _none_if_blank(row.get("id"))
                name = _none_if_blank(row.get("first_name"))
                dept = _none_if_blank(row.get("department")) or ""
                date = _none_if_blank(row.get("date"))
                if code is None or name is None or date is None:
                    continue

                emp = db.query(Employee).filter(Employee.employee_code == str(code)).first()
                if emp is None:
                    emp = Employee(employee_code=str(code), name=str(name), dept=str(dept), job_title="Employee")
                    db.add(emp)
                    db.flush()
                    employees_upserted += 1
                else:
                    changed = False
                    if emp.name != str(name):
                        emp.name = str(name)
                        changed = True
                    if dept and emp.dept != str(dept):
                        emp.dept = str(dept)
                        changed = True
                    if emp.job_title is None:
                        emp.job_title = "Employee"
                        changed = True
                    if changed:
                        employees_upserted += 1

                rec = db.query(AttendanceRecord).filter(
                    AttendanceRecord.employee_id == emp.id,
                    AttendanceRecord.date == str(date),
                ).first()

                payload = {
                    "week": _none_if_blank(row.get("week")),
                    "actual_check_in_time": _none_if_blank(row.get("actual_check-in_time")) or _none_if_blank(row.get("actual_check_in_time")),
                    "actual_check_out_time": _none_if_blank(row.get("actual_check-out_time")) or _none_if_blank(row.get("actual_check_out_time")),
                    "attendance_records": _none_if_blank(row.get("attendance_records")),
                    "total_work_hours": _none_if_blank(row.get("total_work_hours")),
                    "total_work_minutes": _parse_work_minutes(row.get("total_work_hours")),
                    "attendance_status": _none_if_blank(row.get("attendance_status")),
                    "ot1": _to_float(row.get("ot1")),
                    "ot2": _to_float(row.get("ot2")),
                    "ot3": _to_float(row.get("ot3")),
                    "sick_leave": _to_float(row.get("sick_leave")),
                    "maternity_leave": _to_float(row.get("maternity_leave")),
                    "annual_leave": _to_float(row.get("annual_leave")),
                    "personal_leave": _to_float(row.get("personal_leave")),
                    "paternity_leave": _to_float(row.get("paternity_leave")),
                    "parental_leave": _to_float(row.get("parental_leave")),
                    "family_reunion_leave": _to_float(row.get("family_reunion_leave")),
                    "bereavement_leave": _to_float(row.get("bereavement_leave")),
                    "business_trip": _to_float(row.get("business_trip")),
                    "overtime_exchange_holiday": _to_float(row.get("overtime_exchange_holiday")),
                    "business_trip_exchange_holiday": _to_float(row.get("business_trip_exchange_holiday")),
                }

                if rec is None:
                    rec = AttendanceRecord(employee_id=emp.id, date=str(date), **payload)
                    db.add(rec)
                    attendance_rows_upserted += 1
                else:
                    for k, v in payload.items():
                        setattr(rec, k, v)
                    attendance_rows_upserted += 1

            _recompute_monthly_stats(db, _extract_year_months(distinct_dates))
        else:
            raise ValueError("Unsupported Excel format for upload.")

        db.commit()

    return {
        "file": file_name,
        "employees_upserted": employees_upserted,
        "attendance_rows_upserted": attendance_rows_upserted,
        "distinct_dates": distinct_dates,
        "detected_format": "test0-hik" if detected_test0_hik else "unknown",
    }

def _extract_year_months(distinct_dates: List[str]) -> List[str]:
    months = []
    seen = set()
    for d in distinct_dates:
        s = str(d)
        if len(s) >= 7:
            ym = s[:7]
            if ym not in seen:
                seen.add(ym)
                months.append(ym)
    return months

def _recompute_monthly_stats(db: Session, year_months: List[str]) -> None:
    for ym in year_months:
        rows = (
            db.query(
                AttendanceRecord.employee_id.label("employee_id"),
                func.coalesce(func.sum(AttendanceRecord.total_work_minutes), 0).label("total_work_minutes"),
                func.count(AttendanceRecord.id).label("days_total"),
                func.coalesce(func.sum(case((AttendanceRecord.total_work_minutes > 0, 1), else_=0)), 0).label("days_present"),
                func.coalesce(func.sum(func.coalesce(AttendanceRecord.annual_leave, 0.0)), 0.0).label("annual_leave_days"),
                func.coalesce(func.sum(func.coalesce(AttendanceRecord.personal_leave, 0.0)), 0.0).label("casual_leave_days"),
                func.coalesce(
                    func.sum(
                        func.coalesce(AttendanceRecord.ot1, 0.0) +
                        func.coalesce(AttendanceRecord.ot2, 0.0) +
                        func.coalesce(AttendanceRecord.ot3, 0.0)
                    ),
                    0.0,
                ).label("ot_total"),
            )
            .filter(AttendanceRecord.date.like(f"{ym}%"))
            .group_by(AttendanceRecord.employee_id)
            .all()
        )

        for r in rows:
            existing = (
                db.query(MonthlyEmployeeStats)
                .filter(MonthlyEmployeeStats.employee_id == r.employee_id, MonthlyEmployeeStats.year_month == ym)
                .first()
            )
            days_absent = int(r.days_total or 0) - int(r.days_present or 0)
            if existing is None:
                db.add(
                    MonthlyEmployeeStats(
                        employee_id=r.employee_id,
                        year_month=ym,
                        total_work_minutes=int(r.total_work_minutes or 0),
                        days_present=int(r.days_present or 0),
                        days_absent=int(days_absent),
                        annual_leave_days=float(r.annual_leave_days or 0.0),
                        casual_leave_days=float(r.casual_leave_days or 0.0),
                        ot_total=float(r.ot_total or 0.0),
                    )
                )
            else:
                existing.total_work_minutes = int(r.total_work_minutes or 0)
                existing.days_present = int(r.days_present or 0)
                existing.days_absent = int(days_absent)
                existing.annual_leave_days = float(r.annual_leave_days or 0.0)
                existing.casual_leave_days = float(r.casual_leave_days or 0.0)
                existing.ot_total = float(r.ot_total or 0.0)

if __name__ == "__main__":
    res = import_from_excel()
    print(
        f"[OK] Imported from '{res['file']}' format={res['detected_format']} "
        f"employees_upserted={res['employees_upserted']} attendance_rows_upserted={res['attendance_rows_upserted']}"
    )

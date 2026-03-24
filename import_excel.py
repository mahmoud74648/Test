import pandas as pd
import re
import io
from typing import Optional, Tuple, Dict, Any, List
from sqlalchemy import func, case
from database import engine
from models import Base, Department, Employee, AttendanceRecord, MonthlyEmployeeStats, Leave, Permission
from sqlalchemy.orm import Session

DEFAULT_EXCEL_FILE = "test0-hik.xlsx"

def _normalize_col(c: Any) -> str:
    s = str(c).strip().lower()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    return s.strip("_")

def _none_if_blank(v: Any):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, str):
        s = v.replace("\ufeff", " ").replace("\u200f", " ").replace("\u200e", " ").replace("\xa0", " ").strip()
        if s in {"", "-", "nan"}:
            return None
        return s
    return v

def _to_code_str(v: Any) -> Optional[str]:
    v = _none_if_blank(v)
    if v is None:
        return None
    if isinstance(v, float):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if float(v).is_integer():
            return str(int(v))
        return str(v).strip()
    if isinstance(v, int):
        return str(v)
    s = str(v).strip()
    if not s or s.lower() in {"nan", "-"}:
        return None
    return s

def _unique_preserve_order(items: List[Any]) -> List[Any]:
    seen = set()
    out: List[Any] = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

def _to_int(v: Any) -> Optional[int]:
    v = _none_if_blank(v)
    if v is None:
        return None
    try:
        if isinstance(v, float) and pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return int(float(v))
    except Exception:
        return None

def _sanitize_employee_data(data: Dict[str, Any]) -> Dict[str, Any]:
    date_fields = {"join_date", "contract_end_date"}
    float_fields = {"salary", "leave_allowance_annual_days", "leave_allowance_casual_days"}
    int_fields = {"rests"}
    code_like_fields = {"id", "employee_code", "national_id", "insurance_number", "phone", "iban"}

    out: Dict[str, Any] = {}
    for k, v in data.items():
        if k in date_fields:
            out[k] = _to_date_str(v)
            continue
        if k in float_fields:
            out[k] = _to_float(v)
            continue
        if k in int_fields:
            out[k] = _to_int(v)
            continue
        if k in code_like_fields:
            out[k] = _to_code_str(v)
            continue
        vv = _none_if_blank(v)
        if vv is None:
            out[k] = None
        else:
            out[k] = str(vv)
    return out

def _to_float(v: Any) -> Optional[float]:
    v = _none_if_blank(v)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None

def _to_date_str(v: Any) -> Optional[str]:
    v = _none_if_blank(v)
    if v is None:
        return None
    if isinstance(v, pd.Timestamp):
        try:
            return v.strftime("%Y-%m-%d")
        except Exception:
            return str(v)[:10]
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%Y-%m-%d")
        except Exception:
            pass
    s = str(v).strip()
    if not s:
        return None
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return pd.Timestamp(ts).strftime("%Y-%m-%d")
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

def _detect_header_row_for_employee_list_preview(preview: pd.DataFrame) -> Optional[int]:
    need = {
        "employee_code",
        "code",
        "id",
        "emp_code",
        "employee_id",
        "employee_no",
        "employee_number",
        "national_id",
        "الرقم_القومي",
        "الرقم_القومى",
        "رقم_الموظف",
        "رقم_موظف",
        "رقم_الكود",
        "رقم_كود",
        "الكود",
        "كود",
        "كود_الموظف",
        "مسلسل",
        "م",
        "name",
        "الاسم",
        "الإسم",
        "اسم",
        "اسم_الموظف",
        "dept",
        "department",
        "الإدارة",
        "الادارة",
        "الاداره",
        "القسم",
        "الوظيفة",
        "الوظيفه",
        "تاريخ_التعيين",
        "رقم_الموبايل",
        "المؤهل",
    }
    best: Tuple[int, int] | None = None
    for i in range(len(preview)):
        row = {_normalize_col(c) for c in preview.iloc[i].tolist() if _none_if_blank(c) is not None}
        score = len(need & row)
        if score >= 3 and (best is None or score > best[1]):
            best = (i, score)
    return best[0] if best else None

def _detect_header_row_for_employee_list(file: str, max_scan_rows: int = 40) -> Optional[int]:
    preview = pd.read_excel(file, header=None, nrows=max_scan_rows)
    return _detect_header_row_for_employee_list_preview(preview)

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
    header_row = _detect_header_row_for_employee_list(file) or 0
    df = pd.read_excel(file, header=header_row, dtype=str)
    df.columns = [_normalize_col(c) for c in df.columns]
    if "join_date" not in df.columns and "تاريخ_التعيين" not in df.columns:
        if "سنة" in df.columns and "شهر" in df.columns and "يوم" in df.columns:
            y = pd.to_numeric(df["سنة"], errors="coerce")
            m = pd.to_numeric(df["شهر"], errors="coerce")
            d = pd.to_numeric(df["يوم"], errors="coerce")
            join = pd.to_datetime(
                pd.DataFrame({"year": y, "month": m, "day": d}),
                errors="coerce",
            )
            df["join_date"] = join.dt.strftime("%Y-%m-%d")
    column_map = {
        "id": "id",
        "employee_code": "employee_code",
        "code": "employee_code",
        "emp_code": "employee_code",
        "employee_id": "employee_code",
        "employee_no": "employee_code",
        "employee_number": "employee_code",
        "رقم_الموظف": "employee_code",
        "رقم_موظف": "employee_code",
        "رقم_الكود": "employee_code",
        "رقم_كود": "employee_code",
        "كود": "employee_code",
        "كود_الموظف": "employee_code",
        "الكود": "employee_code",
        "مسلسل": "employee_code",
        "م": "employee_code",

        "name": "name",
        "الاسم": "name",
        "الإسم": "name",
        "اسم": "name",
        "اسم_الموظف": "name",

        "dept": "dept",
        "department": "dept",
        "الإدارة": "dept",
        "الادارة": "dept",
        "الاداره": "dept",
        "ادارة": "dept",
        "القسم": "dept",

        "job_title": "job_title",
        "job": "job_title",
        "المسمى": "job_title",
        "المسمى_الوظيفي": "job_title",
        "الوظيفة": "job_title",
        "الوظيفه": "job_title",

        "phone": "phone",
        "الهاتف": "phone",
        "موبايل": "phone",
        "mobile": "phone",
        "رقم_الموبايل": "phone",
        "رقم_المحمول": "phone",

        "email": "email",

        "salary": "salary",
        "المرتب": "salary",

        "join_date": "join_date",
        "تاريخ_التعيين": "join_date",
        "تاريخ_بدء_العمل": "join_date",

        "contract_end_date": "contract_end_date",
        "تاريخ_انتهاء_العقد": "contract_end_date",
        "تاريخ_نهاية_العقد": "contract_end_date",

        "leave_allowance_annual_days": "leave_allowance_annual_days",
        "annual_leave_allowance": "leave_allowance_annual_days",
        "annual_allowance": "leave_allowance_annual_days",
        "رصيد_سنوي": "leave_allowance_annual_days",
        "رصيد_سنوية": "leave_allowance_annual_days",
        "رصيد_السنوي": "leave_allowance_annual_days",

        "leave_allowance_casual_days": "leave_allowance_casual_days",
        "casual_leave_allowance": "leave_allowance_casual_days",
        "casual_allowance": "leave_allowance_casual_days",
        "رصيد_عارض": "leave_allowance_casual_days",
        "رصيد_عارضة": "leave_allowance_casual_days",
        "رصيد_العارض": "leave_allowance_casual_days",

        "rests": "rests",
        "الراحات": "rests",
        "عدد_الراحات": "rests",

        "status": "status",
        "الحالة": "status",

        "national_id": "national_id",
        "الرقم_القومي": "national_id",
        "الرقم_القومى": "national_id",

        "insurance_number": "insurance_number",
        "رقم_التأمين": "insurance_number",
        "التأمين": "insurance_number",

        "education": "education",
        "المؤهل": "education",
        "المؤهل_الدراسي": "education",
        "المؤهل_الدراسى": "education",

        "university": "university",
        "الجامعة": "university",
        "جهة_الحصول": "university",

        "marital_status": "marital_status",
        "الحالة_الاجتماعية": "marital_status",
        "الحالة_الإجتماعية": "marital_status",

        "religion": "religion",
        "الديانة": "religion",

        "governorate": "governorate",
        "المحافظة": "governorate",

        "city": "city",
        "المركز": "city",
        "المدينة": "city",

        "area": "area",
        "القرية": "area",
        "الحي": "area",
        "المنطقة": "area",

        "address": "address",
        "العنوان": "address",

        "iban": "iban",
        "ايبان": "iban",
        "الايبان": "iban",
    }
    df = df.rename(columns=column_map)
    known = _unique_preserve_order(list(column_map.values()))
    return df[[c for c in known if c in df.columns]]

def _read_employee_list_bytes(content: bytes) -> pd.DataFrame:
    preview = pd.read_excel(io.BytesIO(content), header=None, nrows=40)
    header_row = _detect_header_row_for_employee_list_preview(preview) or 0
    df = pd.read_excel(io.BytesIO(content), header=header_row, dtype=str)
    df.columns = [_normalize_col(c) for c in df.columns]
    if "join_date" not in df.columns and "تاريخ_التعيين" not in df.columns:
        if "سنة" in df.columns and "شهر" in df.columns and "يوم" in df.columns:
            y = pd.to_numeric(df["سنة"], errors="coerce")
            m = pd.to_numeric(df["شهر"], errors="coerce")
            d = pd.to_numeric(df["يوم"], errors="coerce")
            join = pd.to_datetime(
                pd.DataFrame({"year": y, "month": m, "day": d}),
                errors="coerce",
            )
            df["join_date"] = join.dt.strftime("%Y-%m-%d")
    column_map = {
        "id": "id",
        "employee_code": "employee_code",
        "code": "employee_code",
        "emp_code": "employee_code",
        "employee_id": "employee_code",
        "employee_no": "employee_code",
        "employee_number": "employee_code",
        "رقم_الموظف": "employee_code",
        "رقم_موظف": "employee_code",
        "رقم_الكود": "employee_code",
        "رقم_كود": "employee_code",
        "كود": "employee_code",
        "كود_الموظف": "employee_code",
        "الكود": "employee_code",
        "مسلسل": "employee_code",
        "م": "employee_code",

        "name": "name",
        "الاسم": "name",
        "الإسم": "name",
        "اسم": "name",
        "اسم_الموظف": "name",

        "dept": "dept",
        "department": "dept",
        "الإدارة": "dept",
        "الادارة": "dept",
        "الاداره": "dept",
        "ادارة": "dept",
        "القسم": "dept",

        "job_title": "job_title",
        "job": "job_title",
        "المسمى": "job_title",
        "المسمى_الوظيفي": "job_title",
        "الوظيفة": "job_title",
        "الوظيفه": "job_title",

        "phone": "phone",
        "الهاتف": "phone",
        "موبايل": "phone",
        "mobile": "phone",
        "رقم_الموبايل": "phone",
        "رقم_المحمول": "phone",

        "email": "email",

        "salary": "salary",
        "المرتب": "salary",

        "join_date": "join_date",
        "تاريخ_التعيين": "join_date",
        "تاريخ_بدء_العمل": "join_date",

        "contract_end_date": "contract_end_date",
        "تاريخ_انتهاء_العقد": "contract_end_date",
        "تاريخ_نهاية_العقد": "contract_end_date",

        "leave_allowance_annual_days": "leave_allowance_annual_days",
        "annual_leave_allowance": "leave_allowance_annual_days",
        "annual_allowance": "leave_allowance_annual_days",
        "رصيد_سنوي": "leave_allowance_annual_days",
        "رصيد_سنوية": "leave_allowance_annual_days",
        "رصيد_السنوي": "leave_allowance_annual_days",

        "leave_allowance_casual_days": "leave_allowance_casual_days",
        "casual_leave_allowance": "leave_allowance_casual_days",
        "casual_allowance": "leave_allowance_casual_days",
        "رصيد_عارض": "leave_allowance_casual_days",
        "رصيد_عارضة": "leave_allowance_casual_days",
        "رصيد_العارض": "leave_allowance_casual_days",

        "rests": "rests",
        "الراحات": "rests",
        "عدد_الراحات": "rests",

        "status": "status",
        "الحالة": "status",

        "national_id": "national_id",
        "الرقم_القومي": "national_id",
        "الرقم_القومى": "national_id",

        "insurance_number": "insurance_number",
        "رقم_التأمين": "insurance_number",
        "التأمين": "insurance_number",

        "education": "education",
        "المؤهل": "education",
        "المؤهل_الدراسي": "education",
        "المؤهل_الدراسى": "education",

        "university": "university",
        "الجامعة": "university",
        "جهة_الحصول": "university",

        "marital_status": "marital_status",
        "الحالة_الاجتماعية": "marital_status",
        "الحالة_الإجتماعية": "marital_status",

        "religion": "religion",
        "الديانة": "religion",

        "governorate": "governorate",
        "المحافظة": "governorate",

        "city": "city",
        "المركز": "city",
        "المدينة": "city",

        "area": "area",
        "القرية": "area",
        "الحي": "area",
        "المنطقة": "area",

        "address": "address",
        "العنوان": "address",

        "iban": "iban",
        "ايبان": "iban",
        "الايبان": "iban",
    }
    df = df.rename(columns=column_map)
    known = _unique_preserve_order(list(column_map.values()))
    return df[[c for c in known if c in df.columns]]

def _read_permissions_list_bytes(content: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(content))
    df.columns = [_normalize_col(c) for c in df.columns]
    column_map = {
        "employee_code": "employee_code",
        "code": "employee_code",
        "id": "employee_code",
        "الكود": "employee_code",
        "مسلسل": "employee_code",
        "م": "employee_code",
        "رقم_الموظف": "employee_code",
        "رقم_موظف": "employee_code",

        "date": "date",
        "التاريخ": "date",
        "يوم": "date",

        "hours": "hours",
        "hour": "hours",
        "مدة": "hours",
        "المدة": "hours",
        "عدد_الساعات": "hours",
        "ساعات": "hours",

        "note": "note",
        "ملاحظة": "note",
        "ملاحظات": "note",
        "reason": "note",
        "سبب": "note",
    }
    df = df.rename(columns=column_map)
    keep = [c for c in ["employee_code", "date", "hours", "note"] if c in df.columns]
    return df[keep]

def _read_leaves_list_bytes(content: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(content))
    df.columns = [_normalize_col(c) for c in df.columns]
    column_map = {
        "employee_code": "employee_code",
        "code": "employee_code",
        "id": "employee_code",
        "الكود": "employee_code",
        "مسلسل": "employee_code",
        "م": "employee_code",
        "رقم_الموظف": "employee_code",
        "رقم_موظف": "employee_code",

        "date": "date",
        "التاريخ": "date",

        "days": "days",
        "day": "days",
        "عدد_الايام": "days",
        "عدد_الأيام": "days",
        "ايام": "days",
        "أيام": "days",

        "leave_type": "leave_type",
        "type": "leave_type",
        "النوع": "leave_type",
        "نوع": "leave_type",
        "اجازة": "leave_type",
        "إجازة": "leave_type",

        "note": "note",
        "ملاحظة": "note",
        "ملاحظات": "note",
        "reason": "note",
        "سبب": "note",
    }
    df = df.rename(columns=column_map)
    keep = [c for c in ["employee_code", "date", "days", "leave_type", "note"] if c in df.columns]
    return df[keep]

def import_employees_only_from_excel_bytes(content: bytes, file_name: str = "employees.xlsx") -> Dict[str, Any]:
    Base.metadata.create_all(bind=engine)
    employees_upserted = 0
    rows_skipped = 0
    employees_created = 0
    employees_updated = 0
    departments_added = 0
    skipped_missing_code = 0
    skipped_missing_name_or_dept = 0
    skipped_missing_code_rows: List[int] = []
    skipped_missing_name_or_dept_rows: List[int] = []

    df = _read_employee_list_bytes(content)
    with Session(engine) as db:
        existing_depts = {r[0] for r in db.query(Department.name).all() if r and r[0]}
        for i, row in df.iterrows():
            data: Dict[str, Any] = {}
            for k in list(dict.fromkeys(df.columns.tolist())):
                v = row[k]
                if isinstance(v, pd.Series):
                    chosen = None
                    for x in v.tolist():
                        if _none_if_blank(x) is not None:
                            chosen = x
                            break
                    data[k] = chosen
                else:
                    data[k] = None if pd.isna(v) else v
            data = _sanitize_employee_data(data)
            code_emp = _to_code_str(data.get("employee_code"))
            code_id = _to_code_str(data.get("id"))
            code_national = _to_code_str(data.get("national_id"))
            code = code_emp or code_id or code_national
            if code_national is not None and (code is None or len(str(code).strip()) <= 6):
                code = code_national
            if code is None:
                skipped_missing_code += 1
                rows_skipped += 1
                if len(skipped_missing_code_rows) < 10:
                    skipped_missing_code_rows.append(int(i) + 1)
                continue

            emp = db.query(Employee).filter(Employee.employee_code == str(code)).first()
            if emp is None:
                name = _none_if_blank(data.get("name"))
                dept = _none_if_blank(data.get("dept"))
                if name is None or dept is None:
                    skipped_missing_name_or_dept += 1
                    rows_skipped += 1
                    if len(skipped_missing_name_or_dept_rows) < 10:
                        skipped_missing_name_or_dept_rows.append(int(i) + 1)
                    continue
                dept_name = str(dept).strip()
                if dept_name:
                    if dept_name not in existing_depts:
                        db.add(Department(name=dept_name))
                        existing_depts.add(dept_name)
                        departments_added += 1
                create_data = {k: data.get(k) for k in data.keys() if k not in {"id"}}
                create_data["employee_code"] = str(code)
                create_data["name"] = str(name)
                create_data["dept"] = str(dept)
                emp = Employee(**create_data)
                db.add(emp)
                employees_upserted += 1
                employees_created += 1
                continue

            for k, v in data.items():
                if k in {"employee_code", "id"}:
                    continue
                if v is not None:
                    setattr(emp, k, v)
            dept_val = _none_if_blank(data.get("dept"))
            if dept_val is not None:
                dept_name = str(dept_val).strip()
                if dept_name:
                    if dept_name not in existing_depts:
                        db.add(Department(name=dept_name))
                        existing_depts.add(dept_name)
                        departments_added += 1
            employees_upserted += 1
            employees_updated += 1

        db.commit()

    return {
        "file": file_name,
        "rows_total": int(len(df)),
        "detected_columns": sorted({str(c) for c in df.columns}),
        "employees_upserted": employees_upserted,
        "employees_created": employees_created,
        "employees_updated": employees_updated,
        "departments_added": departments_added,
        "rows_skipped": rows_skipped,
        "skipped_missing_code": skipped_missing_code,
        "skipped_missing_name_or_dept": skipped_missing_name_or_dept,
        "skipped_missing_code_rows": skipped_missing_code_rows,
        "skipped_missing_name_or_dept_rows": skipped_missing_name_or_dept_rows,
    }

def import_permissions_from_excel_bytes(content: bytes, file_name: str = "permissions.xlsx") -> Dict[str, Any]:
    Base.metadata.create_all(bind=engine)
    permissions_inserted = 0
    rows_skipped = 0
    affected_employee_ids: set[int] = set()
    affected_employee_codes: set[str] = set()

    df = _read_permissions_list_bytes(content)
    with Session(engine) as db:
        for _, row in df.iterrows():
            code = _to_code_str(row.get("employee_code"))
            date = _to_date_str(row.get("date"))
            hours = _to_float(row.get("hours"))
            note = _none_if_blank(row.get("note")) or ""

            if code is None or date is None or hours is None:
                rows_skipped += 1
                continue
            if hours <= 0:
                rows_skipped += 1
                continue

            emp = db.query(Employee).filter(Employee.employee_code == str(code)).first()
            if emp is None:
                rows_skipped += 1
                continue
            affected_employee_ids.add(int(emp.id))
            affected_employee_codes.add(str(emp.employee_code))

            exists = (
                db.query(Permission)
                .filter(
                    Permission.employee_id == emp.id,
                    Permission.date == date,
                    Permission.hours == float(hours),
                    Permission.note == note,
                )
                .first()
            )
            if exists is not None:
                rows_skipped += 1
                continue

            db.add(Permission(employee_id=emp.id, date=date, hours=float(hours), note=note))
            permissions_inserted += 1

        db.commit()

    return {
        "file": file_name,
        "permissions_inserted": permissions_inserted,
        "rows_skipped": rows_skipped,
        "affected_employee_ids": sorted(affected_employee_ids),
        "affected_employee_codes": sorted(affected_employee_codes),
    }

def import_leaves_from_excel_bytes(content: bytes, file_name: str = "leaves.xlsx") -> Dict[str, Any]:
    Base.metadata.create_all(bind=engine)
    leaves_inserted = 0
    rows_skipped = 0
    affected_employee_ids: set[int] = set()
    affected_employee_codes: set[str] = set()

    df = _read_leaves_list_bytes(content)
    with Session(engine) as db:
        for _, row in df.iterrows():
            code = _to_code_str(row.get("employee_code"))
            date = _to_date_str(row.get("date"))
            days = _to_float(row.get("days"))
            lt_raw = _none_if_blank(row.get("leave_type"))
            note = _none_if_blank(row.get("note")) or ""

            if code is None or date is None or days is None or lt_raw is None:
                rows_skipped += 1
                continue
            if days <= 0:
                rows_skipped += 1
                continue

            lt = str(lt_raw).strip().lower()
            if lt in {"سنوية", "سنوى", "سنوي", "annual"}:
                lt = "annual"
            elif lt in {"عارضة", "عارضه", "casual"}:
                lt = "casual"
            if lt not in {"annual", "casual"}:
                rows_skipped += 1
                continue

            emp = db.query(Employee).filter(Employee.employee_code == str(code)).first()
            if emp is None:
                rows_skipped += 1
                continue
            affected_employee_ids.add(int(emp.id))
            affected_employee_codes.add(str(emp.employee_code))

            exists = (
                db.query(Leave)
                .filter(
                    Leave.employee_id == emp.id,
                    Leave.date == date,
                    Leave.leave_type == lt,
                    Leave.days == float(days),
                    Leave.note == note,
                )
                .first()
            )
            if exists is not None:
                rows_skipped += 1
                continue

            db.add(Leave(employee_id=emp.id, date=date, leave_type=lt, days=float(days), note=note))
            leaves_inserted += 1

        db.commit()

    return {
        "file": file_name,
        "leaves_inserted": leaves_inserted,
        "rows_skipped": rows_skipped,
        "affected_employee_ids": sorted(affected_employee_ids),
        "affected_employee_codes": sorted(affected_employee_codes),
    }

def import_from_excel(file: str = DEFAULT_EXCEL_FILE, reset_db: bool = True, allow_create_employees: bool = True) -> Dict[str, Any]:
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
                    if not allow_create_employees:
                        continue
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

def import_from_excel_bytes(content: bytes, file_name: str = "upload.xlsx", reset_db: bool = False, allow_create_employees: bool = True) -> Dict[str, Any]:
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
                    if not allow_create_employees:
                        continue
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
            df = _read_employee_list_bytes(content)
            for _, row in df.iterrows():
                data = {k: (None if pd.isna(v) else v) for k, v in row.items()}
                code = _none_if_blank(data.get("employee_code"))
                if code is None:
                    code = _none_if_blank(data.get("id"))
                if code is not None:
                    emp = db.query(Employee).filter(Employee.employee_code == str(code)).first()
                    if emp is None:
                        emp = Employee(**{k: data.get(k) for k in data.keys() if k != "id"})
                        emp.employee_code = str(code)
                        db.add(emp)
                        employees_upserted += 1
                    else:
                        for k, v in data.items():
                            if k in {"employee_code", "id"}:
                                continue
                            if v is not None:
                                setattr(emp, k, v)
                        employees_upserted += 1
                else:
                    emp = Employee(**{k: data.get(k) for k in data.keys() if k != "id"})
                    db.add(emp)
                    employees_upserted += 1

        db.commit()

    return {
        "file": file_name,
        "employees_upserted": employees_upserted,
        "attendance_rows_upserted": attendance_rows_upserted,
        "distinct_dates": distinct_dates,
        "detected_format": "test0-hik" if detected_test0_hik else "employee-list",
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

from pydantic import BaseModel
from typing import Optional, List, Literal

class EmployeeBase(BaseModel):
    employee_code: Optional[str] = None
    name:      str
    dept:      str
    job_title: Optional[str] = None
    phone:     Optional[str] = None
    email:     Optional[str] = None
    salary:    Optional[float] = 0.0
    join_date: Optional[str] = None
    contract_end_date: Optional[str] = None
    rests:     Optional[int] = 0
    status:    Optional[str] = "active"
    leave_allowance_annual_days: Optional[float] = 0.0
    leave_allowance_casual_days: Optional[float] = 0.0
    national_id: Optional[str] = None
    insurance_number: Optional[str] = None
    education: Optional[str] = None
    university: Optional[str] = None
    marital_status: Optional[str] = None
    religion: Optional[str] = None
    governorate: Optional[str] = None
    city: Optional[str] = None
    area: Optional[str] = None
    address: Optional[str] = None
    iban: Optional[str] = None

class EmployeeCreate(EmployeeBase):
    pass

class EmployeeUpdate(BaseModel):
    employee_code: Optional[str] = None
    name:      Optional[str] = None
    dept:      Optional[str] = None
    job_title: Optional[str] = None
    phone:     Optional[str] = None
    email:     Optional[str] = None
    salary:    Optional[float] = None
    join_date: Optional[str] = None
    contract_end_date: Optional[str] = None
    rests:     Optional[int] = None
    status:    Optional[str] = None
    leave_allowance_annual_days: Optional[float] = None
    leave_allowance_casual_days: Optional[float] = None
    national_id: Optional[str] = None
    insurance_number: Optional[str] = None
    education: Optional[str] = None
    university: Optional[str] = None
    marital_status: Optional[str] = None
    religion: Optional[str] = None
    governorate: Optional[str] = None
    city: Optional[str] = None
    area: Optional[str] = None
    address: Optional[str] = None
    iban: Optional[str] = None

class EmployeeOut(EmployeeBase):
    id: int

    class Config:
        from_attributes = True


class AttendanceRecordBase(BaseModel):
    date: str
    week: Optional[str] = None

    actual_check_in_time: Optional[str] = None
    actual_check_out_time: Optional[str] = None
    attendance_records: Optional[str] = None
    total_work_hours: Optional[str] = None
    total_work_minutes: Optional[int] = None
    attendance_status: Optional[str] = None

    ot1: Optional[float] = None
    ot2: Optional[float] = None
    ot3: Optional[float] = None

    sick_leave: Optional[float] = None
    maternity_leave: Optional[float] = None
    annual_leave: Optional[float] = None
    personal_leave: Optional[float] = None
    paternity_leave: Optional[float] = None
    parental_leave: Optional[float] = None
    family_reunion_leave: Optional[float] = None
    bereavement_leave: Optional[float] = None
    business_trip: Optional[float] = None
    overtime_exchange_holiday: Optional[float] = None
    business_trip_exchange_holiday: Optional[float] = None


class AttendanceRecordOut(AttendanceRecordBase):
    id: int
    employee_id: int

    class Config:
        from_attributes = True


class AttendanceRowOut(BaseModel):
    employee_code: str
    name: Optional[str] = None
    dept: Optional[str] = None
    date: str
    actual_check_in_time: Optional[str] = None
    actual_check_out_time: Optional[str] = None
    attendance_records: Optional[str] = None
    total_work_hours: Optional[str] = None


class LeaveRowOut(BaseModel):
    employee_code: str
    name: Optional[str] = None
    dept: Optional[str] = None
    date: str
    annual_leave: float = 0.0
    personal_leave: float = 0.0


class MonthlyStatsOut(BaseModel):
    employee_code: str
    name: Optional[str] = None
    dept: Optional[str] = None
    year_month: str
    total_work_minutes: int
    days_present: int
    days_absent: int
    annual_leave_days: float
    casual_leave_days: float
    ot_total: float


class MtdStatsOut(BaseModel):
    employee_code: str
    name: Optional[str] = None
    dept: Optional[str] = None
    year_month: str
    to_date: str
    days_present: int
    days_absent: int
    delays: int
    permissions: int
    annual_leave_days: float
    casual_leave_days: float


class ImportExcelRequest(BaseModel):
    file: str = "test0-hik.xlsx"
    reset_db: bool = True


class ImportExcelResult(BaseModel):
    file: str
    employees_upserted: int
    attendance_rows_upserted: int
    distinct_dates: List[str]


# ── Manual Leave ───────────────────────────────────────────────────────────────
class LeaveCreate(BaseModel):
    leave_type: str          # "annual" | "casual"
    days: float = 1.0
    date: str                # YYYY-MM-DD
    note: Optional[str] = ""

class LeaveOut(LeaveCreate):
    id: int
    employee_id: int
    created_at: Optional[str] = None
    class Config:
        from_attributes = True


# ── Manual Permission ──────────────────────────────────────────────────────────
class PermissionCreate(BaseModel):
    hours: float = 1.0
    date: str
    note: Optional[str] = ""

class PermissionOut(PermissionCreate):
    id: int
    employee_id: int
    created_at: Optional[str] = None
    class Config:
        from_attributes = True


# ── Manual Deduction ───────────────────────────────────────────────────────────
class DeductionCreate(BaseModel):
    deduction_type: Literal["days"] = "days"
    amount: float
    reason: Optional[str] = ""
    date: str

class DeductionOut(DeductionCreate):
    id: int
    employee_id: int
    created_at: Optional[str] = None
    class Config:
        from_attributes = True


# ── Employee Summary ───────────────────────────────────────────────────────────
class EmployeeSummaryOut(BaseModel):
    id: int
    employee_code: Optional[str] = None
    name: str
    dept: str
    job_title: Optional[str] = None
    status: Optional[str] = None
    leave_allowance_annual_days: float = 0.0
    leave_allowance_casual_days: float = 0.0
    total_annual_leave: float = 0.0
    total_casual_leave: float = 0.0
    total_leave_used_days: float = 0.0
    total_permission_hours: float = 0.0
    total_deduction_days: float = 0.0
    total_deduction_money: float = 0.0
    annual_leave_remaining_days: float = 0.0
    casual_leave_remaining_days: float = 0.0
    leave_remaining_days: float = 0.0

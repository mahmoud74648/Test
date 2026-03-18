from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint, Text, DateTime
from datetime import datetime
from sqlalchemy.orm import relationship
from database import Base

class Employee(Base):
    __tablename__ = "employees"

    id         = Column(Integer, primary_key=True, index=True)
    employee_code = Column(String, unique=True, index=True)
    name       = Column(String, nullable=False)
    dept       = Column(String, nullable=False)
    job_title  = Column(String)
    phone      = Column(String)
    email      = Column(String)
    salary     = Column(Float, default=0.0)
    join_date  = Column(String)
    rests      = Column(Integer, default=0)
    status     = Column(String, default="active")   # active | inactive

    attendance_records = relationship(
        "AttendanceRecord",
        back_populates="employee",
        cascade="all, delete-orphan",
    )
    leaves = relationship("Leave", back_populates="employee", cascade="all, delete-orphan")
    permissions = relationship("Permission", back_populates="employee", cascade="all, delete-orphan")
    deductions = relationship("Deduction", back_populates="employee", cascade="all, delete-orphan")


class AttendanceRecord(Base):
    __tablename__ = "attendance_records"
    __table_args__ = (
        UniqueConstraint("employee_id", "date", name="uq_attendance_employee_date"),
    )

    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(Integer, ForeignKey("employees.id", ondelete="CASCADE"), index=True, nullable=False)

    date = Column(String, index=True, nullable=False)
    week = Column(String)

    actual_check_in_time = Column(String)
    actual_check_out_time = Column(String)
    attendance_records = Column(String)
    total_work_hours = Column(String)
    total_work_minutes = Column(Integer)
    attendance_status = Column(String)

    ot1 = Column(Float)
    ot2 = Column(Float)
    ot3 = Column(Float)

    sick_leave = Column(Float)
    maternity_leave = Column(Float)
    annual_leave = Column(Float)
    personal_leave = Column(Float)
    paternity_leave = Column(Float)
    parental_leave = Column(Float)
    family_reunion_leave = Column(Float)
    bereavement_leave = Column(Float)
    business_trip = Column(Float)
    overtime_exchange_holiday = Column(Float)
    business_trip_exchange_holiday = Column(Float)

    employee = relationship("Employee", back_populates="attendance_records")


class MonthlyEmployeeStats(Base):
    __tablename__ = "monthly_employee_stats"
    __table_args__ = (
        UniqueConstraint("employee_id", "year_month", name="uq_monthly_stats_employee_month"),
    )

    id = Column(Integer, primary_key=True, index=True)
    employee_id = Column(Integer, ForeignKey("employees.id", ondelete="CASCADE"), index=True, nullable=False)
    year_month = Column(String, index=True, nullable=False)

    total_work_minutes = Column(Integer, default=0)
    days_present = Column(Integer, default=0)
    days_absent = Column(Integer, default=0)

    annual_leave_days = Column(Float, default=0.0)
    casual_leave_days = Column(Float, default=0.0)
    ot_total = Column(Float, default=0.0)

    employee = relationship("Employee")


class Leave(Base):
    """Manual leave records (annual / casual) added by HR staff."""
    __tablename__ = "leaves_manual"

    id          = Column(Integer, primary_key=True, index=True)
    employee_id = Column(Integer, ForeignKey("employees.id", ondelete="CASCADE"), index=True, nullable=False)
    leave_type  = Column(String, nullable=False)   # "annual" | "casual"
    days        = Column(Float, nullable=False, default=1.0)
    date        = Column(String, nullable=False)   # YYYY-MM-DD
    note        = Column(Text, default="")
    created_at  = Column(String, default=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    employee = relationship("Employee", back_populates="leaves")


class Permission(Base):
    """Manual permission (إذن) records in hours."""
    __tablename__ = "permissions_manual"

    id          = Column(Integer, primary_key=True, index=True)
    employee_id = Column(Integer, ForeignKey("employees.id", ondelete="CASCADE"), index=True, nullable=False)
    hours       = Column(Float, nullable=False, default=1.0)
    date        = Column(String, nullable=False)   # YYYY-MM-DD
    note        = Column(Text, default="")
    created_at  = Column(String, default=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    employee = relationship("Employee", back_populates="permissions")


class Deduction(Base):
    """Manual deduction records – can be days or money."""
    __tablename__ = "deductions_manual"

    id             = Column(Integer, primary_key=True, index=True)
    employee_id    = Column(Integer, ForeignKey("employees.id", ondelete="CASCADE"), index=True, nullable=False)
    deduction_type = Column(String, nullable=False, default="money")  # "days" | "money"
    amount         = Column(Float, nullable=False, default=0.0)       # days or EGP
    reason         = Column(Text, default="")
    date           = Column(String, nullable=False)
    created_at     = Column(String, default=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    employee = relationship("Employee", back_populates="deductions")

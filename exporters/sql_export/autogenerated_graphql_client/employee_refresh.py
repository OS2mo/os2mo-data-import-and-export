from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class EmployeeRefresh(BaseModel):
    employee_refresh: "EmployeeRefreshEmployeeRefresh"


class EmployeeRefreshEmployeeRefresh(BaseModel):
    objects: List[UUID]
    page_info: "EmployeeRefreshEmployeeRefreshPageInfo"


class EmployeeRefreshEmployeeRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


EmployeeRefresh.update_forward_refs()
EmployeeRefreshEmployeeRefresh.update_forward_refs()
EmployeeRefreshEmployeeRefreshPageInfo.update_forward_refs()

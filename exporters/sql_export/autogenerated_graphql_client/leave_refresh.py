from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class LeaveRefresh(BaseModel):
    leave_refresh: "LeaveRefreshLeaveRefresh"


class LeaveRefreshLeaveRefresh(BaseModel):
    objects: List[UUID]
    page_info: "LeaveRefreshLeaveRefreshPageInfo"


class LeaveRefreshLeaveRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


LeaveRefresh.update_forward_refs()
LeaveRefreshLeaveRefresh.update_forward_refs()
LeaveRefreshLeaveRefreshPageInfo.update_forward_refs()

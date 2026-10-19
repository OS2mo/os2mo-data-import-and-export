from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class ManagerRefresh(BaseModel):
    manager_refresh: "ManagerRefreshManagerRefresh"


class ManagerRefreshManagerRefresh(BaseModel):
    objects: List[UUID]
    page_info: "ManagerRefreshManagerRefreshPageInfo"


class ManagerRefreshManagerRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


ManagerRefresh.update_forward_refs()
ManagerRefreshManagerRefresh.update_forward_refs()
ManagerRefreshManagerRefreshPageInfo.update_forward_refs()

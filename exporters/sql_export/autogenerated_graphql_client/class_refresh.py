from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class ClassRefresh(BaseModel):
    class_refresh: "ClassRefreshClassRefresh"


class ClassRefreshClassRefresh(BaseModel):
    objects: List[UUID]
    page_info: "ClassRefreshClassRefreshPageInfo"


class ClassRefreshClassRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


ClassRefresh.update_forward_refs()
ClassRefreshClassRefresh.update_forward_refs()
ClassRefreshClassRefreshPageInfo.update_forward_refs()

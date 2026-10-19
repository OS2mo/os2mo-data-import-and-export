from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class ItsystemRefresh(BaseModel):
    itsystem_refresh: "ItsystemRefreshItsystemRefresh"


class ItsystemRefreshItsystemRefresh(BaseModel):
    objects: List[UUID]
    page_info: "ItsystemRefreshItsystemRefreshPageInfo"


class ItsystemRefreshItsystemRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


ItsystemRefresh.update_forward_refs()
ItsystemRefreshItsystemRefresh.update_forward_refs()
ItsystemRefreshItsystemRefreshPageInfo.update_forward_refs()

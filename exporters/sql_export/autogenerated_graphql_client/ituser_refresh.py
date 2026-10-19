from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class ItuserRefresh(BaseModel):
    ituser_refresh: "ItuserRefreshItuserRefresh"


class ItuserRefreshItuserRefresh(BaseModel):
    objects: List[UUID]
    page_info: "ItuserRefreshItuserRefreshPageInfo"


class ItuserRefreshItuserRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


ItuserRefresh.update_forward_refs()
ItuserRefreshItuserRefresh.update_forward_refs()
ItuserRefreshItuserRefreshPageInfo.update_forward_refs()

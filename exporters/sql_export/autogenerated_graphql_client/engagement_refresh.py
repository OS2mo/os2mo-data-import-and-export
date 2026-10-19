from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class EngagementRefresh(BaseModel):
    engagement_refresh: "EngagementRefreshEngagementRefresh"


class EngagementRefreshEngagementRefresh(BaseModel):
    objects: List[UUID]
    page_info: "EngagementRefreshEngagementRefreshPageInfo"


class EngagementRefreshEngagementRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


EngagementRefresh.update_forward_refs()
EngagementRefreshEngagementRefresh.update_forward_refs()
EngagementRefreshEngagementRefreshPageInfo.update_forward_refs()

from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class RelatedUnitRefresh(BaseModel):
    related_unit_refresh: "RelatedUnitRefreshRelatedUnitRefresh"


class RelatedUnitRefreshRelatedUnitRefresh(BaseModel):
    objects: List[UUID]
    page_info: "RelatedUnitRefreshRelatedUnitRefreshPageInfo"


class RelatedUnitRefreshRelatedUnitRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


RelatedUnitRefresh.update_forward_refs()
RelatedUnitRefreshRelatedUnitRefresh.update_forward_refs()
RelatedUnitRefreshRelatedUnitRefreshPageInfo.update_forward_refs()

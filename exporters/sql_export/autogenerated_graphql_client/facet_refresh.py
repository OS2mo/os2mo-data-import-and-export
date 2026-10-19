from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class FacetRefresh(BaseModel):
    facet_refresh: "FacetRefreshFacetRefresh"


class FacetRefreshFacetRefresh(BaseModel):
    objects: List[UUID]
    page_info: "FacetRefreshFacetRefreshPageInfo"


class FacetRefreshFacetRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


FacetRefresh.update_forward_refs()
FacetRefreshFacetRefresh.update_forward_refs()
FacetRefreshFacetRefreshPageInfo.update_forward_refs()

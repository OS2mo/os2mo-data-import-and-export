from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class AssociationRefresh(BaseModel):
    association_refresh: "AssociationRefreshAssociationRefresh"


class AssociationRefreshAssociationRefresh(BaseModel):
    objects: List[UUID]
    page_info: "AssociationRefreshAssociationRefreshPageInfo"


class AssociationRefreshAssociationRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


AssociationRefresh.update_forward_refs()
AssociationRefreshAssociationRefresh.update_forward_refs()
AssociationRefreshAssociationRefreshPageInfo.update_forward_refs()

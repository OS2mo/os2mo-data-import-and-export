from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class OrgUnitRefresh(BaseModel):
    org_unit_refresh: "OrgUnitRefreshOrgUnitRefresh"


class OrgUnitRefreshOrgUnitRefresh(BaseModel):
    objects: List[UUID]
    page_info: "OrgUnitRefreshOrgUnitRefreshPageInfo"


class OrgUnitRefreshOrgUnitRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


OrgUnitRefresh.update_forward_refs()
OrgUnitRefreshOrgUnitRefresh.update_forward_refs()
OrgUnitRefreshOrgUnitRefreshPageInfo.update_forward_refs()

from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class KleRefresh(BaseModel):
    kle_refresh: "KleRefreshKleRefresh"


class KleRefreshKleRefresh(BaseModel):
    objects: List[UUID]
    page_info: "KleRefreshKleRefreshPageInfo"


class KleRefreshKleRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


KleRefresh.update_forward_refs()
KleRefreshKleRefresh.update_forward_refs()
KleRefreshKleRefreshPageInfo.update_forward_refs()

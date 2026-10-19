from typing import Any, List, Optional
from uuid import UUID

from .base_model import BaseModel


class AddressRefresh(BaseModel):
    address_refresh: "AddressRefreshAddressRefresh"


class AddressRefreshAddressRefresh(BaseModel):
    objects: List[UUID]
    page_info: "AddressRefreshAddressRefreshPageInfo"


class AddressRefreshAddressRefreshPageInfo(BaseModel):
    next_cursor: Optional[Any]


AddressRefresh.update_forward_refs()
AddressRefreshAddressRefresh.update_forward_refs()
AddressRefreshAddressRefreshPageInfo.update_forward_refs()

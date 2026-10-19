from typing import Any, Optional, Union
from uuid import UUID

from .address_refresh import AddressRefresh, AddressRefreshAddressRefresh
from .association_refresh import (
    AssociationRefresh,
    AssociationRefreshAssociationRefresh,
)
from .async_base_client import AsyncBaseClient
from .base_model import UNSET, UnsetType
from .class_refresh import ClassRefresh, ClassRefreshClassRefresh
from .employee_refresh import EmployeeRefresh, EmployeeRefreshEmployeeRefresh
from .engagement_refresh import EngagementRefresh, EngagementRefreshEngagementRefresh
from .facet_refresh import FacetRefresh, FacetRefreshFacetRefresh
from .itsystem_refresh import ItsystemRefresh, ItsystemRefreshItsystemRefresh
from .ituser_refresh import ItuserRefresh, ItuserRefreshItuserRefresh
from .kle_refresh import KleRefresh, KleRefreshKleRefresh
from .leave_refresh import LeaveRefresh, LeaveRefreshLeaveRefresh
from .manager_refresh import ManagerRefresh, ManagerRefreshManagerRefresh
from .me import Me, MeMe
from .org_unit_refresh import OrgUnitRefresh, OrgUnitRefreshOrgUnitRefresh
from .related_unit_refresh import (
    RelatedUnitRefresh,
    RelatedUnitRefreshRelatedUnitRefresh,
)


def gql(q: str) -> str:
    return q


class GraphQLClient(AsyncBaseClient):

    async def me(self) -> MeMe:
        query = gql("""
            query Me {
              me {
                actor {
                  uuid
                }
              }
            }
            """)
        variables: dict[str, object] = {}
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return Me.parse_obj(data).me

    async def address_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> AddressRefreshAddressRefresh:
        query = gql("""
            mutation address_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              address_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return AddressRefresh.parse_obj(data).address_refresh

    async def association_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> AssociationRefreshAssociationRefresh:
        query = gql("""
            mutation association_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              association_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return AssociationRefresh.parse_obj(data).association_refresh

    async def class_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> ClassRefreshClassRefresh:
        query = gql("""
            mutation class_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              class_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return ClassRefresh.parse_obj(data).class_refresh

    async def employee_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> EmployeeRefreshEmployeeRefresh:
        query = gql("""
            mutation employee_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              employee_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return EmployeeRefresh.parse_obj(data).employee_refresh

    async def engagement_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> EngagementRefreshEngagementRefresh:
        query = gql("""
            mutation engagement_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              engagement_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return EngagementRefresh.parse_obj(data).engagement_refresh

    async def facet_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> FacetRefreshFacetRefresh:
        query = gql("""
            mutation facet_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              facet_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return FacetRefresh.parse_obj(data).facet_refresh

    async def itsystem_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> ItsystemRefreshItsystemRefresh:
        query = gql("""
            mutation itsystem_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              itsystem_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return ItsystemRefresh.parse_obj(data).itsystem_refresh

    async def ituser_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> ItuserRefreshItuserRefresh:
        query = gql("""
            mutation ituser_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              ituser_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return ItuserRefresh.parse_obj(data).ituser_refresh

    async def kle_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> KleRefreshKleRefresh:
        query = gql("""
            mutation kle_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              kle_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return KleRefresh.parse_obj(data).kle_refresh

    async def leave_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> LeaveRefreshLeaveRefresh:
        query = gql("""
            mutation leave_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              leave_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return LeaveRefresh.parse_obj(data).leave_refresh

    async def manager_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> ManagerRefreshManagerRefresh:
        query = gql("""
            mutation manager_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              manager_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return ManagerRefresh.parse_obj(data).manager_refresh

    async def org_unit_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> OrgUnitRefreshOrgUnitRefresh:
        query = gql("""
            mutation org_unit_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              org_unit_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return OrgUnitRefresh.parse_obj(data).org_unit_refresh

    async def related_unit_refresh(
        self, owner: UUID, limit: Any, cursor: Union[Optional[Any], UnsetType] = UNSET
    ) -> RelatedUnitRefreshRelatedUnitRefresh:
        query = gql("""
            mutation related_unit_refresh($owner: UUID!, $cursor: Cursor = null, $limit: int!) {
              related_unit_refresh(owner: $owner, cursor: $cursor, limit: $limit) {
                objects
                page_info {
                  next_cursor
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "owner": owner,
            "cursor": cursor,
            "limit": limit,
        }
        response = await self.execute(query=query, variables=variables)
        data = self.get_data(response)
        return RelatedUnitRefresh.parse_obj(data).related_unit_refresh

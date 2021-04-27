from csv import DictReader
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from more_itertools import flatten
from pydantic import BaseModel, PrivateAttr

from os2mo_data_import.Clients.MO.model import OrgUnit
from os2mo_data_import.util import generate_uuid


@dataclass(frozen=True)
class PartialManager:
    engagement_user_key: str
    org_unit_uuid: UUID


class RawOrgUnit(BaseModel):
    Business_Level_2: str
    Business_Level_3: str
    Business_Level_4: str
    Business_Level_5: str
    Business_Level_6: str
    Business_Level_7: str
    Business_Level_8: str
    Business_Level_9: str
    Org_Unit_No: int
    Manager_Init: str
    Rowcounter: int
    __path: List[str] = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)
        self.__path = list(
            filter(
                bool,
                [
                    self.Business_Level_2,
                    self.Business_Level_3,
                    self.Business_Level_4,
                    self.Business_Level_5,
                    self.Business_Level_6,
                    self.Business_Level_7,
                    self.Business_Level_8,
                    self.Business_Level_9,
                ],
            )
        )

    def level(self) -> int:
        """
        used to sort by level
        :return:
        """

        return len(self.__path)

    @staticmethod
    def __spaces_to_underscores(obj: Dict[str, Any]) -> Dict[str, Any]:
        return dict(
            map(lambda keyval: (keyval[0].replace(" ", "_"), keyval[1]), obj.items())
        )

    @classmethod
    def from_dict_with_spaces_in_keys(cls, obj: Dict[str, Any]) -> "RawOrgUnit":
        """
        should be replaced with Field(alias="...") but I didn't know about that

        :param obj:
        :return:
        """
        obj = cls.parse_obj(cls.__spaces_to_underscores(obj))
        return obj

    def __get_uuid(self) -> UUID:
        return generate_uuid(str(self.__path))

    def __get_parent_uuid(self) -> Optional[UUID]:
        if not self.__path[1:]:
            return None
        return generate_uuid(str(self.__path[:-1]))

    def to_mo_org_unit(
            self, org_unit_type_uuid: UUID, org_unit_level_uuid: UUID
    ) -> OrgUnit:
        return OrgUnit.from_simplified_fields(
            uuid=self.__get_uuid(),
            user_key=str(self.Org_Unit_No),
            name=self.__path[-1],
            parent_uuid=self.__get_parent_uuid(),
            org_unit_type_uuid=org_unit_type_uuid,
            org_unit_level_uuid=org_unit_level_uuid,
        )

    def get_partial_manager(self) -> PartialManager:
        return PartialManager(
            engagement_user_key=self.Manager_Init, org_unit_uuid=self.__get_uuid()
        )


def read_csv(
        path: Path, org_unit_type_uuid: UUID, org_unit_level_uuid: UUID
) -> Tuple[List[List[OrgUnit]], List[PartialManager]]:
    with path.open("r") as file:
        raw_org_units = list(
            map(RawOrgUnit.from_dict_with_spaces_in_keys, DictReader(file))
        )
        raw_org_units = list(sorted(raw_org_units, key=lambda x: x.level()))

        raw_org_units = [
            list(group)
            for _, group in groupby(raw_org_units, key=lambda x: x.level())
        ]

        ous = [
            list(
                map(
                    lambda unit: unit.to_mo_org_unit(
                        org_unit_type_uuid=org_unit_type_uuid,
                        org_unit_level_uuid=org_unit_level_uuid,
                    ),
                    group,
                )
            )
            for group in raw_org_units
        ]
        # shitty transformation to list of single-element-lists, shouldn't need this,
        # but I can't get it to work without
        ret = [[ou] for ou in flatten(ous)]

        return ret, list(
            map(lambda x: x.get_partial_manager(), flatten(raw_org_units))
        )


if __name__ == "__main__":
    with Path("/home/mw/gir/gir_orsted_hierarki_linje.csv").open("r") as file:
        RawOrgUnits = map(RawOrgUnit.from_dict_with_spaces_in_keys, DictReader(file))
        for x in RawOrgUnits:
            print(x)
            exit()

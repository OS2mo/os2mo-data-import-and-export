from abc import ABCMeta, abstractmethod
from typing import Optional

from pydantic import BaseModel, Extra


class ConfiguredBase(BaseModel):
    class Config:
        allow_mutation = False
        frozen = True
        allow_population_by_field_name = True
        extra = Extra.forbid


class LoraObj(ConfiguredBase, metaclass=ABCMeta):
    @abstractmethod
    def get_uuid(self) -> Optional[str]:
        """
        All MoObjs should be able to implement this.
        :return: If an Obj does not have one, returns None
        """
        return None

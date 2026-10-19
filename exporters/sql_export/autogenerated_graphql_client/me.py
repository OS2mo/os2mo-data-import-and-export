from uuid import UUID

from .base_model import BaseModel


class Me(BaseModel):
    me: "MeMe"


class MeMe(BaseModel):
    actor: "MeMeActor"


class MeMeActor(BaseModel):
    uuid: UUID


Me.update_forward_refs()
MeMe.update_forward_refs()
MeMeActor.update_forward_refs()

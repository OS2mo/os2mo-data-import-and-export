# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0

from uuid import UUID

from pydantic import BaseModel


class MOSDToolPayload(BaseModel):
    """MO SDTool payload."""

    class Config:
        schema_extra = {
            "example": {
                "uuid": "fb2d158f-114e-5f67-8365-2c520cf10b58",
            }
        }

    uuid: UUID

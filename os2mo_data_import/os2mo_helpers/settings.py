#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
# --------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
from functools import lru_cache
from typing import Optional

from pydantic import AnyHttpUrl
from pydantic import BaseSettings
from pydantic import Field

# --------------------------------------------------------------------------------------
# MoraHelper settings
# --------------------------------------------------------------------------------------


class MoraSettings(BaseSettings):
    client_id: str = "dipex"
    client_secret: Optional[str]
    auth_realm: str = "mo"
    auth_server: AnyHttpUrl = Field("http://localhost:8081/auth")
    saml_token: Optional[str]

    def get_token_url(self) -> str:
        return (
            f"{self.auth_server}/realms/{self.auth_realm}/protocol/openid-connect/token"
        )


@lru_cache
def get_settings() -> MoraSettings:
    return MoraSettings()


if __name__ == "__main__":
    print(MoraSettings())

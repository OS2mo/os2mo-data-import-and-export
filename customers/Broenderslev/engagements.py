#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
from typing import Any, Dict, List

import pandas as pd
import requests
from more_itertools import one

from exporters.utils.load_settings import load_settings

# --------------------------------------------------------------------------------------
# Get user engagements
# --------------------------------------------------------------------------------------


class UserEngagements:
    def __init__(self, host: str, saml: str, org_name: str) -> None:
        self.host = host.strip("/")
        self.header = {"SESSION": saml}
        get_org = requests.get(f"{self.host}/service/o/", headers=self.header)
        get_org.raise_for_status()
        org = one(get_org.json())
        if org_name not in org["name"]:
            raise ValueError(
                f"Organisation {org_name} not found. Host returned {org['name']}."
            )
        self.org_uuid = org["uuid"]

    def _get_all_users(self) -> List[Dict[str, Any]]:
        get_users = requests.get(
            f"{self.host}/service/o/{self.org_uuid}/e/", headers=self.header
        )
        get_users.raise_for_status()
        return get_users.json()["items"]

    def get_user_engagements(self) -> pd.DataFrame:
        user_list = self._get_all_users()
        for user in user_list:
            user_uuid = user["uuid"]
            get_engagements = requests.get(
                f"{self.host}/service/e/{user_uuid}/details/engagement",
                headers=self.header,
            )
            get_engagements.raise_for_status()
            engagements = [
                {"user_key": eng.get("user_key")} for eng in get_engagements.json()
            ]
            user.update({"engagements": engagements})

        user_data = pd.json_normalize(
            user_list,
            record_path=["engagements"],
            record_prefix="engagements.",
            meta=["uuid", "name"],
            meta_prefix="user.",
        )
        return user_data


def main() -> None:
    settings = load_settings()
    user_eng = UserEngagements(
        settings["mora.base"], settings["crontab.SAML_TOKEN"], "Brønderslev"
    ).get_user_engagements()
    uuid_re = (
        r"^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b"
        r"-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$"
    )
    filtered_users = user_eng[user_eng["engagements.user_key"].str.match(uuid_re)]
    print(filtered_users.to_csv(index=False))


if __name__ == "__main__":
    main()

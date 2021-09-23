#!/usr/bin/env python3
# --------------------------------------------------------------------------------------
# SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
# --------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
from ra_utils.load_settings import load_settings

from reports.query_actualstate import list_org_units, run_report

# --------------------------------------------------------------------------------------
# Code
# --------------------------------------------------------------------------------------


def main() -> None:
    settings = load_settings()
    query_path = settings["mora.folder.query_export"]
    run_report(
        list_org_units,
        "Organsiationsenheder",
        "Svendborg Kommune",
        query_path + "/Organisationsenheder.xlsx",
    )


if __name__ == "__main__":
    main()

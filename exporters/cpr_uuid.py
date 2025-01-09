from collections.abc import Iterator
from typing import Any
from uuid import UUID

import click
from os2mo_helpers.mora_helpers import MoraHelper
from pydantic import BaseModel
from ra_utils.load_settings import load_setting
from ra_utils.tqdm_wrapper import tqdm


class ExportUser(BaseModel):
    cpr: str | None
    mo_uuid: UUID
    ad_guid: UUID | None
    sam_account_name: str | None


def cache_ad_reader() -> Any:
    from integrations.ad_integration.ad_reader import ADParameterReader

    print("Caching all users from AD...")
    ad_reader = ADParameterReader()
    ad_reader.cache_all(print_progress=True)
    print("OK")
    return ad_reader


def create_mapping(helper: MoraHelper, use_ad: bool) -> Iterator[ExportUser]:

    print("Fetching all users from MO...")
    employees = helper.read_all_users()
    print("OK")

    ad_reader = cache_ad_reader() if use_ad else None

    print("Processing all...")
    for employee in tqdm(employees):
        # AD properties will be enriched if available
        cpr = employee.get("cpr_no")
        if not cpr:
            print("no 'cpr_no' for MO user %r", employee["uuid"])
        user = ExportUser(
            cpr=cpr,
            mo_uuid=employee["uuid"],
        )

        if ad_reader:
            ad_info = ad_reader.read_user(cpr=user.cpr, cache_only=True)
            if ad_info:
                user.ad_guid = ad_info["ObjectGuid"]
                user.sam_account_name = ad_info["SamAccountName"]

        yield user

    print("OK")


def main(mora_base: str, use_ad: bool, output_file_path: str) -> None:
    mh = MoraHelper(hostname=mora_base, export_ansi=False)

    employees = list(create_mapping(mh, use_ad))
    employee_dicts = [x.dict() for x in employees]

    fields = ["cpr", "mo_uuid", "ad_guid", "sam_account_name"]
    mh._write_csv(fields, employee_dicts, output_file_path)


@click.command()
@click.option(
    "--mora-base",
    default=load_setting("mora.base", "http://localhost:5000"),
    help="URL for OS2mo.",
)
@click.option(
    "--use-ad",
    default=False,
    is_flag=True,
    help="Enrich with AD data.",
)
@click.option(
    "--output-file-path",
    default="cpr_mo_ad_map.csv",
    type=click.Path(),
    help="Path to write output file to.",
    show_default=True,
)
def cli(mora_base: str, use_ad: bool, output_file_path: str) -> None:
    """MO CPR, MO UUID, AD GUID, AD SAM CSV Exporter."""
    main(mora_base, use_ad, output_file_path)


if __name__ == "__main__":
    cli()

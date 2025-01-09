from collections.abc import Iterator
from typing import Any

import click
import requests
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_setting
from ra_utils.tqdm_wrapper import tqdm


def cache_ad_reader() -> Any:
    from integrations.ad_integration.ad_reader import ADParameterReader

    print("Caching all users from AD...")
    ad_reader = ADParameterReader()
    ad_reader.cache_all(print_progress=True)
    print("OK")
    return ad_reader


def create_mapping(
    helper: MoraHelper, use_ad: bool, new_ldap_url: str | None
) -> Iterator[dict[str, Any]]:
    print("Fetching all users from MO...")
    employees = helper.read_all_users()
    print("OK")

    ad_reader = cache_ad_reader() if use_ad else None

    print("Processing all...")
    for employee in tqdm(employees):
        uuid = employee["uuid"]

        # AD properties will be enriched if available
        cpr = employee.get("cpr_no")
        if not cpr:
            print("no 'cpr_no' for MO user %r", employee["uuid"])

        ad_guid = None
        sam_account_name = None

        if ad_reader:
            ad_info = ad_reader.read_user(cpr=cpr, cache_only=True)
            if ad_info:
                ad_guid = ad_info["ObjectGuid"]
                sam_account_name = ad_info["SamAccountName"]

        if new_ldap_url:
            r = requests.get(new_ldap_url, params={"uuid": uuid})
            r.raise_for_status()
            result = r.json()
            ad_guid = result["uuid"]
            sam_account_name = result["username"]

        yield {
            "cpr": cpr,
            "mo_uuid": uuid,
            "ad_guid": ad_guid,
            "sam_account_name": sam_account_name,
        }

    print("OK")


def main(
    mora_base: str, use_ad: bool, new_ldap_url: str | None, output_file_path: str
) -> None:
    mh = MoraHelper(hostname=mora_base, export_ansi=False)

    employee_dicts = list(create_mapping(mh, use_ad, new_ldap_url))

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
    "--new-ldap-url",
    default=None,
    help="Utilize new LDAP integration, with the provided url",
)
@click.option(
    "--output-file-path",
    default="cpr_mo_ad_map.csv",
    type=click.Path(),
    help="Path to write output file to.",
    show_default=True,
)
def cli(
    mora_base: str, use_ad: bool, new_ldap_url: str | None, output_file_path: str
) -> None:
    """MO CPR, MO UUID, AD GUID, AD SAM CSV Exporter."""
    main(mora_base, use_ad, new_ldap_url, output_file_path)


if __name__ == "__main__":
    cli()

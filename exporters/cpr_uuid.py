import logging
from functools import partial
from operator import methodcaller
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

import click
from os2mo_helpers.mora_helpers import MoraHelper
from pydantic import BaseModel
from ra_utils.load_settings import load_setting
from tqdm import tqdm


logger = logging.getLogger("cpr_uuid")


class ExportUser(BaseModel):
    cpr: str
    mo_uuid: UUID
    ad_guid: Optional[UUID]
    sam_account_name: Optional[str]


def create_mapping(helper, use_ad) -> List[ExportUser]:
    def cache_ad_reader() -> Any:
        from integrations.ad_integration.ad_reader import ADParameterReader

        print("Caching all users from AD...")
        ad_reader = ADParameterReader()
        ad_reader.cache_all(print_progress=True)
        print("OK")
        return ad_reader

    def enrich_user_dict_from_ad(ad_reader: Any, user: ExportUser) -> ExportUser:
        ad_info = ad_reader.read_user(cpr=user.cpr, cache_only=True)
        if ad_info:
            user.ad_guid = ad_info["ObjectGuid"]
            user.sam_account_name = ad_info["SamAccountName"]
        return user

    def to_user_dict(employee: Dict[str, Any]) -> ExportUser:
        # AD properties will be enriched if available
        return ExportUser(
            cpr=employee["cpr_no"],
            mo_uuid=employee["uuid"],
        )

    print("Fetching all users from MO...")
    employees = helper.read_all_users()
    total = len(employees)
    print("OK")

    employees = map(to_user_dict, employees)

    if use_ad:
        ad_reader = cache_ad_reader()
        employees = map(partial(enrich_user_dict_from_ad, ad_reader), employees)

    print("Processing all...")
    employees = tqdm(employees, total=total)
    employees = list(employees)
    print("OK")
    return employees


def main(mora_base: str, use_ad: bool, output_file_path: str) -> None:
    mh = MoraHelper(hostname=mora_base, export_ansi=True)

    employees: List[ExportUser] = create_mapping(mh, use_ad)
    employee_dicts: List[Dict] = list(map(methodcaller("dict"), employees))

    fields = ["cpr", "mo_uuid", "ad_guid", "sam_account_name"]
    mh._write_csv(fields, employee_dicts, output_file_path)


def init_log(log_path: str) -> None:
    LOG_LEVEL = logging.DEBUG

    # detail_logging = ('AdCommon', 'mora-helper', 'AdReader', 'cpr_uuid')
    detail_logging = ("mora-helper", "AdReader", "cpr_uuid")
    for name in logging.root.manager.loggerDict:  # type: ignore
        if name in detail_logging:
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
        level=LOG_LEVEL,
        filename=log_path,
    )


@click.command(help="UUID exporter")
@click.option(
    "--mora-base",
    default=load_setting("mora.base", "http://localhost:5000"),
    required=True,
    help="URL for OS2mo.",
)
@click.option(
    "--use-ad",
    default=False,
    is_flag=True,
    required=True,
    help="Enrich with AD data.",
)
@click.option(
    "--log-file-path",
    default="cpr_uuid_export.log",
    required=True,
    type=click.Path(),
    help="Path to write log file.",
    show_default=True,
)
@click.option(
    "--output-file-path",
    default="cpr_mo_ad_map.csv",
    required=True,
    type=click.Path(),
    help="Path to write output file to.",
    show_default=True,
)
def cli(
    mora_base: str, use_ad: bool, log_file_path: str, output_file_path: str
) -> None:
    init_log(log_file_path)
    main(mora_base, use_ad, output_file_path)


if __name__ == "__main__":
    cli()

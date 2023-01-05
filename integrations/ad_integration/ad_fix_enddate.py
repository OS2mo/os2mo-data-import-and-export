import datetime
import logging
import typing
from datetime import date
from datetime import datetime as dt

import click
import dateutil.parser
import sentry_sdk
from fastapi.encoders import jsonable_encoder
from gql import gql
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.job_settings import JobSettings
from ra_utils.load_settings import load_setting
from ra_utils.tqdm_wrapper import tqdm
from raclients.graph.client import GraphQLClient
from raclients.graph.client import SyncClientSession

from integrations.ad_integration.ad_common import AD
from integrations.ad_integration.ad_reader import ADParameterReader


class AdFixEndDateSettings(JobSettings):
    class Config:
        settings_json_prefix = "integrations.ad.write"


logger = logging.getLogger(__name__)


class CompareEndDate(ADParameterReader):
    def __init__(
        self,
        enddate_field: str,
        uuid_field: str,
        graph_ql_session: SyncClientSession,
        settings: typing.Optional[dict] = None,
    ):
        super().__init__(all_settings=settings)
        self.helper = MoraHelper(
            hostname=self.all_settings["global"]["mora.base"], use_cache=False
        )
        self.enddate_field = enddate_field
        self.uuid_field = uuid_field
        self.graph_ql_session: SyncClientSession = graph_ql_session
        self.ad_null_date = datetime.date(9999, 12, 31)

    def to_enddate(self, date_str: typing.Optional[str]) -> date:
        """
        Takes a string and converts it to a date, also takes into consideration that when an engagement does not have
        an end date, MO handles it as None, while AD handles it as 9999-12-31
        """
        if not date_str:
            return self.ad_null_date
        end_date = dateutil.parser.parse(date_str).date()
        if end_date.year == self.ad_null_date.year:
            return self.ad_null_date
        return end_date

    def get_employee_end_date(self, uuid: str) -> date:
        query = gql(
            """
            query Get_mo_engagements($to_date: DateTime, $employees: [UUID!]) {
                engagements(from_date: null, to_date: $to_date, employees: $employees) {
                    objects {
                        validity {
                            to
                        }
                    }
                }
            }
            """
        )

        result = self.graph_ql_session.execute(
            query,
            variable_values=jsonable_encoder(
                {"to_date": dt.now().astimezone(), "employees": uuid}
            ),
        )

        if not result["engagements"]:
            raise KeyError("User not found in mo")

        end_dates = [
            self.to_enddate(obj["validity"]["to"])
            for engagement in result["engagements"]
            for obj in engagement["objects"]
        ]

        return max(end_dates)

    def get_end_dates_to_fix(self, show_date_diffs: bool) -> dict:

        # Compare AD users to MO users
        print("Find users from AD")
        ad_users = ADParameterReader.read_it_all(self, print_progress=True)
        end_dates_to_fix = {}
        print("Compare to MO engagement data per user")
        for ad_user in tqdm(ad_users, unit="user"):
            if not (self.uuid_field in ad_user):
                click.echo(
                    f"User with {ad_user['ObjectGuid']=} does not have an {self.uuid_field} field, and will be skipped"
                )
                continue

            uuid = ad_user[self.uuid_field]

            try:
                mo_end_date = self.get_employee_end_date(uuid).strftime("%Y-%m-%d")
            except KeyError:
                continue

            if not (self.enddate_field in ad_user):
                logger.info(
                    "User "
                    + ad_user[self.uuid_field]
                    + " does not have the field "
                    + self.enddate_field
                )
                # if the user does not have an end date, give it one
                end_dates_to_fix[uuid] = mo_end_date
                continue

            if ad_user[self.enddate_field] == mo_end_date:
                continue

            end_dates_to_fix[uuid] = mo_end_date

        if show_date_diffs:
            for ad_user in ad_users:
                if not (self.uuid_field in ad_user):
                    continue

                uuid = ad_user[self.uuid_field]

                if uuid in end_dates_to_fix:

                    if self.enddate_field in ad_user:
                        ad_end = ad_user[self.enddate_field]
                    else:
                        ad_end = "None"

                    logger.info(
                        f"User with id: {uuid} has AD end date: {ad_end} and MO end date: {end_dates_to_fix[uuid]}"
                    )

        return end_dates_to_fix


class UpdateEndDate(AD):
    def __init__(self, enddate_field, uuid_field, settings=None):
        super().__init__(all_settings=settings)
        self.enddate_field = enddate_field
        self.uuid_field = uuid_field

    def get_update_cmd(self, uuid, end_date):
        cmd_f = """
        Get-ADUser %(complete)s -Filter '%(uuid_field)s -eq "%(uuid)s"' |
        Set-ADUser %(credentials)s -Replace @{%(enddate_field)s="%(end_date)s"} |
        ConvertTo-Json
        """
        cmd = cmd_f % dict(
            uuid=uuid,
            end_date=end_date,
            enddate_field=self.enddate_field,
            uuid_field=self.uuid_field,
            complete=self._ps_boiler_plate()["complete"],
            credentials=self._ps_boiler_plate()["credentials"],
        )
        return cmd

    def run(self, cmd) -> dict:
        return self._run_ps_script("%s\n%s" % (self._build_user_credential(), cmd))


@click.command()
@click.option(
    "--enddate-field",
    default=load_setting("integrations.ad_writer.fixup_enddate_field"),
)
@click.option("--uuid-field", default=load_setting("integrations.ad.write.uuid_field"))
@click.option("--dry-run", is_flag=True)
@click.option("--show-date-diffs", is_flag=True)
@click.option("--print-commands", is_flag=True)
@click.option("--mora-base", envvar="MORA_BASE", default="http://mo")
@click.option("--client-id", envvar="CLIENT_ID", default="dipex")
@click.option("--client-secret", envvar="CLIENT_SECRET")
@click.option("--auth-realm", envvar="AUTH_REALM", default="mo")
@click.option("--auth-server", envvar="AUTH_SERVER", default="http://keycloak")
def cli(
    enddate_field,
    uuid_field,
    dry_run,
    show_date_diffs,
    print_commands,
    mora_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
):
    """Fix enddates of terminated users.
    AD-writer does not support writing enddate of a terminated employee,
    this script finds and corrects the enddate in AD of terminated engagements.
    """
    pydantic_settings = AdFixEndDateSettings()
    pydantic_settings.start_logging_based_on_settings()

    if pydantic_settings.sentry_dsn:
        sentry_sdk.init(dsn=pydantic_settings.sentry_dsn)

    logger.info(
        f"Command line args:"
        f" end-date-field = {enddate_field},"
        f" uuid-field = {uuid_field},"
        f" dry-run = {dry_run},"
        f" show-date-diffs = {show_date_diffs},"
        f" print-commands = {print_commands},"
        f" mora-base = {mora_base},"
        f" client-id = {client_id},"
        f" client-secret = not logged,"
        f" auth-realm = {auth_realm},"
        f" auth-server = {auth_server}",
    )

    graph_ql_client = GraphQLClient(
        url=f"{mora_base}/graphql/v3",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    )

    with graph_ql_client as session:
        c = CompareEndDate(
            enddate_field=enddate_field,
            uuid_field=uuid_field,
            graph_ql_session=session,
        )
        end_dates_to_fix = c.get_end_dates_to_fix(show_date_diffs=show_date_diffs)

    u = UpdateEndDate(
        enddate_field=enddate_field,
        uuid_field=uuid_field,
    )

    for uuid, end_date in tqdm(
        end_dates_to_fix.items(), unit="user", desc="Changing enddate in AD"
    ):
        cmd = u.get_update_cmd(uuid, end_date)
        if print_commands:
            logger.info("Command to run: ")
            logger.info(cmd)

        if not dry_run:
            result = u.run(cmd)
            if result:
                logger.info("Result: %r" % result)

    logger.info(f"{len(end_dates_to_fix)} users end dates corrected")

    logger.info("All end dates are fixed")


if __name__ == "__main__":
    cli()

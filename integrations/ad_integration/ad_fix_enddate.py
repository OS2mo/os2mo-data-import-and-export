import datetime
import logging
from dataclasses import dataclass
from typing import Iterator

import click
import httpx
import sentry_sdk
from fastapi.encoders import jsonable_encoder
from gql import gql
from more_itertools import one
from more_itertools import partition
from ra_utils.job_settings import JobSettings
from ra_utils.load_settings import load_setting
from ra_utils.tqdm_wrapper import tqdm
from raclients.graph.client import GraphQLClient
from raclients.graph.client import SyncClientSession
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_delay
from tenacity import wait_fixed

from integrations.ad_integration.ad_common import AD
from integrations.ad_integration.ad_reader import ADParameterReader


logger = logging.getLogger(__name__)


class Unset:
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            return True
        return super().__eq__(other)


class Invalid(Unset):
    pass


class AdFixEndDateSettings(JobSettings):
    class Config:
        settings_json_prefix = "integrations.ad.write"


class MOEngagementDateSource:
    def __init__(self, graphql_session: SyncClientSession):
        self._graphql_session: SyncClientSession = graphql_session

    @retry(
        wait=wait_fixed(5),
        reraise=True,
        stop=stop_after_delay(10 * 60),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def get_employee_engagement_dates(self, uuid: str) -> list[dict]:
        query = gql(
            """
            query Get_mo_engagements($employees: [UUID!]) {
                engagements(employees: $employees, from_date: null, to_date: null) {
                    objects {
                        validity {
                            from
                            to
                        }
                    }
                }
            }
            """
        )

        result = self._graphql_session.execute(
            query,
            variable_values=jsonable_encoder({"employees": uuid}),
        )

        if not result["engagements"]:
            raise KeyError("User not found in mo")

        return result["engagements"]

    def get_end_date(self, uuid: str) -> datetime.datetime | None | Unset:
        try:
            engagements = self.get_employee_engagement_dates(uuid)
        except KeyError:
            return Unset()
        else:
            parsed_validities = self._parse_validities(engagements)
            return max(
                (validity[1] for validity in parsed_validities),
                key=lambda end_datetime: self._fold_in_utc(
                    end_datetime, datetime.datetime.max
                ),
            )

    def get_split_end_dates(
        self, uuid: str
    ) -> tuple[datetime.datetime | Unset, datetime.datetime | Unset]:
        def _split(validity):
            local_date = datetime.datetime.now().astimezone().date()
            start_date = self._fold_in_utc(validity[0], datetime.datetime.min).date()
            return start_date > local_date

        try:
            engagements = self.get_employee_engagement_dates(uuid)
        except KeyError:
            return Unset(), Unset()

        parsed_validities = self._parse_validities(engagements)

        # Split by whether the engagement *starts* in the future, or not
        current, future = partition(_split, parsed_validities)

        # Sort by validity end - validity with latest end appears first in list
        current = sorted(
            current,
            key=lambda validity: self._fold_in_utc(validity[1], datetime.datetime.max),
            reverse=True,
        )

        # Sort by validity start - validity with earliest start appears first in list
        future = sorted(
            future,
            key=lambda validity: self._fold_in_utc(validity[0], datetime.datetime.min),
            reverse=False,
        )

        if current and future:
            # Return end date of latest current validity and earliest future validity
            return current[0][1], future[0][1]
        elif current and not future:
            # Return only end date of latest current validity
            return current[0][1], Unset()
        elif not current and future:
            # Return only end date of earliest future validity
            return Unset(), future[0][1]

        return Unset(), Unset()  # no engagements at all

    def _fold_in_utc(
        self,
        val: datetime.datetime | None,
        default: datetime.datetime,
    ) -> datetime.datetime:
        return (
            val.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            if val is not None
            else default
        )

    def _parse_validities(
        self, engagements
    ) -> list[tuple[datetime.datetime | None, datetime.datetime | None]]:
        def from_iso_or_none(val: str) -> datetime.datetime | None:
            if val:
                maybe_naive_dt = datetime.datetime.fromisoformat(val)
                return maybe_naive_dt.astimezone()
            return None

        def _parse(
            validity: dict,
        ) -> tuple[datetime.datetime | None, datetime.datetime | None]:
            start = from_iso_or_none(validity["from"])
            end = from_iso_or_none(validity["to"])
            folded_start = self._fold_in_utc(start, datetime.datetime.min)
            folded_end = self._fold_in_utc(end, datetime.datetime.max)
            assert folded_start <= folded_end
            return start, end

        parsed_validities = [
            _parse(obj["validity"])
            for engagement in engagements
            for obj in engagement["objects"]
        ]

        return parsed_validities


@dataclass
class ADUserEndDate:
    mo_uuid: str
    field_name: str
    field_value: str | None | Invalid

    @property
    def normalized_value(self) -> datetime.datetime | None | Invalid:
        if self.field_value == Invalid():
            return Invalid()
        try:
            return datetime.datetime.fromisoformat(self.field_value).astimezone()  # type: ignore
        except (TypeError, ValueError):
            logger.debug("cannot parse %r as ISO datetime", self.field_value)
            return None


class ADEndDateSource:
    def __init__(
        self,
        uuid_field: str,
        enddate_field: str,
        enddate_field_future: str | None,
        settings: dict | None = None,
    ):
        self._uuid_field = uuid_field
        self._enddate_field = enddate_field
        self._enddate_field_future = enddate_field_future
        self._reader = ADParameterReader(all_settings=settings)
        self._ad_users: list[dict] = []

    def __iter__(self) -> Iterator[ADUserEndDate]:
        for ad_user in self._ad_users:
            if self._uuid_field not in ad_user:
                click.echo(
                    f"User with {ad_user['ObjectGuid']=} does not have an "
                    f"{self._uuid_field} field, and will be skipped"
                )
                continue

            yield ADUserEndDate(
                ad_user[self._uuid_field],
                self._enddate_field,
                self._get_case_insensitive(ad_user, self._enddate_field),
            )

            if self._enddate_field_future:
                yield ADUserEndDate(
                    ad_user[self._uuid_field],
                    self._enddate_field_future,
                    self._get_case_insensitive(ad_user, self._enddate_field_future),
                )

    def of_all_users(self) -> "ADEndDateSource":
        self._ad_users = self._reader.read_it_all()
        return self

    def of_one_user(self, username: str) -> "ADEndDateSource":
        self._ad_users = [self._reader.read_user(user=username)]
        return self

    def _get_case_insensitive(self, ad_user: dict, field_name: str) -> str | Invalid:
        try:
            return one(
                (v for k, v in ad_user.items() if k.lower() == field_name.lower())
            )
        except ValueError:
            return Invalid()


class CompareEndDate:
    def __init__(
        self,
        enddate_field: str,
        enddate_field_future: str | None,
        mo_engagement_date_source: MOEngagementDateSource,
        ad_end_date_source: ADEndDateSource,
    ):
        self._enddate_field = enddate_field
        self._enddate_field_future = enddate_field_future
        self._mo_engagement_date_source = mo_engagement_date_source
        self._ad_end_date_source = ad_end_date_source
        self._max_date = datetime.datetime.fromisoformat("9999-12-31").astimezone()

    def get_results(self):
        for ad_user in self._ad_end_date_source:
            if self._enddate_field_future:
                split = self._mo_engagement_date_source.get_split_end_dates(
                    ad_user.mo_uuid
                )
                current_mo_end_date, future_mo_end_date = split
                if ad_user.field_name == self._enddate_field:
                    yield ad_user, current_mo_end_date
                if ad_user.field_name == self._enddate_field_future:
                    yield ad_user, future_mo_end_date
            else:
                end_date = self._mo_engagement_date_source.get_end_date(ad_user.mo_uuid)
                if ad_user.field_name == self._enddate_field:
                    yield ad_user, end_date

    def get_changes(self):
        for ad_user, mo_value in self.get_results():
            ad_value = ad_user.normalized_value
            mo_value = mo_value or self._max_date
            if ad_value != mo_value:
                yield ad_user, mo_value
            else:
                logger.info(
                    "MO user %r: normalized MO and AD values are identical in field %r "
                    "(values in MO: %r, AD: %r)",
                    ad_user.mo_uuid,
                    ad_user.field_name,
                    mo_value.strftime("%Y-%m-%d"),
                    ad_user.field_value,
                )


class UpdateEndDate(AD):
    def __init__(self, settings=None):
        super().__init__(all_settings=settings)

    def get_update_cmd(
        self,
        uuid_field: str,
        uuid: str,
        end_date_field: str,
        end_date: str,
    ):
        cmd_f = """
        Get-ADUser %(complete)s -Filter '%(uuid_field)s -eq "%(uuid)s"' |
        Set-ADUser %(credentials)s -Replace @{%(enddate_field)s="%(end_date)s"} |
        ConvertTo-Json
        """
        cmd = cmd_f % dict(
            uuid=uuid,
            end_date=end_date,
            enddate_field=end_date_field,
            uuid_field=uuid_field,
            complete=self._ps_boiler_plate()["complete"],
            credentials=self._ps_boiler_plate()["credentials"],
        )
        return cmd

    def run(self, cmd) -> dict:
        return self._run_ps_script("%s\n%s" % (self._build_user_credential(), cmd))

    def run_all(
        self,
        changes: Iterator[tuple[ADUserEndDate, datetime.datetime]],
        uuid_field: str,
    ):
        changes = tqdm(list(changes))
        num_changes = 0

        for ad_user, mo_value in changes:
            if mo_value == Unset():
                logger.debug(
                    "skipping %r %r as it is unset", ad_user.mo_uuid, ad_user.field_name
                )
            else:
                cmd = self.get_update_cmd(
                    uuid_field,
                    ad_user.mo_uuid,
                    ad_user.field_name,
                    mo_value.strftime("%Y-%m-%d"),
                )
                logger.info(
                    "Updating AD user %r, %r = %r",
                    ad_user.mo_uuid,
                    ad_user.field_name,
                    mo_value.strftime("%Y-%m-%d"),
                )
                result = self.run(cmd)
                if result != {}:
                    raise Exception(result)
                else:
                    num_changes += 1

        logger.info("%d users end dates corrected", num_changes)
        logger.info("All end dates are fixed")

        return retval


@click.command()
@click.option(
    "--enddate-field",
    default=load_setting("integrations.ad_writer.fixup_enddate_field"),
)
@click.option(
    "--enddate-field-future",
    default=load_setting("integrations.ad_writer.fixup_enddate_field_future", None),
)
@click.option("--uuid-field", default=load_setting("integrations.ad.write.uuid_field"))
@click.option(
    "--ad-user",
    help="If given, update only one AD user (specified by username)",
)
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
    enddate_field_future,
    uuid_field,
    ad_user,
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

    graphql_client = GraphQLClient(
        url=f"{mora_base}/graphql/v3",
        client_id=client_id,
        client_secret=client_secret,
        auth_realm=auth_realm,
        auth_server=auth_server,
        sync=True,
        httpx_client_kwargs={"timeout": None},
    )

    ad_end_date_source = ADEndDateSource(
        uuid_field,
        enddate_field,
        enddate_field_future,
    )
    if ad_user:
        ad_end_date_source = ad_end_date_source.of_one_user(ad_user)
    else:
        ad_end_date_source = ad_end_date_source.of_all_users()

    with graphql_client as session:
        mo_engagement_date_source = MOEngagementDateSource(session)
        compare = CompareEndDate(
            enddate_field,
            enddate_field_future,
            mo_engagement_date_source,
            ad_end_date_source,
        )
        update = UpdateEndDate()
        update.run_all(compare.get_changes(), uuid_field)


if __name__ == "__main__":
    cli()

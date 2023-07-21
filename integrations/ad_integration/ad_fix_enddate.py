"""ad_fix_enddate.py

This program takes engagement end dates from MO and writes them to the corresponding AD
user.

The program has evolved from a "one-time fix script" to be responsible for all updates
of end dates in AD. In other words, it has "taken over" writing end dates from
"mo_to_ad_sync.py" (which is unable to "see" past end dates, as it only considers
engagements in the present or future, and thus cannot process terminated engagements.)

The program currently supports two modes of operation:
    a) Writing a single MO end date to a single AD field on each AD user.
    b) Writing two MO end dates to two AD fields on each AD user.

In the first mode, the end date to write is the latest end date of all engagements found
for a given MO user.

In the second mode, the engagement end dates are split into two:
    - the "current" end date is the latest end date occurring in either the past or
      present.
    - the "future" end date is the first end date occurring in the future.
"""
import datetime
import logging
from dataclasses import dataclass
from typing import Any
from typing import Iterable
from typing import Iterator
from typing import Self

import click
import httpx
import sentry_sdk
from dateutil import tz
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

DEFAULT_TIMEZONE = tz.gettz("Europe/Copenhagen")


def in_default_tz(val: datetime.datetime) -> datetime.datetime:
    return val.astimezone(DEFAULT_TIMEZONE)


class _SymbolicConstant:
    # Helper class which is always equal to itself

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            return True
        return super().__eq__(other)


class Unset(_SymbolicConstant):
    """Represents an end date that should *not* be updated in AD.
    Typically because the MO user has no engagements at all, or because the MO user has
    no engagements in the future, or the past/present.

    When encountering an `Unset` end date, the program should not update the matching
    AD field.
    """


class Invalid(_SymbolicConstant):
    """Represents an invalid AD end date, usually due to malformed or unexpected data,
    that cannot be parsed into a `datetime.datetime` object.
    """


class PositiveInfinity(_SymbolicConstant):
    """Represents the upper bound of a validity period if MO returns `None`, i.e. the
    validity period ends at "infinity."
    """

    def as_datetime(self) -> datetime.datetime:
        return in_default_tz(datetime.datetime.fromisoformat("9999-12-31"))


class NegativeInfinity(_SymbolicConstant):
    """Represents the lower bound of a validity period if MO returns `None`, i.e. the
    validity period begins at "-infinity."
    """

    def as_datetime(self) -> datetime.datetime:
        return in_default_tz(datetime.datetime.fromisoformat("1930-01-01"))


ValidityTuple = tuple[
    datetime.datetime | NegativeInfinity,  # from
    datetime.datetime | PositiveInfinity,  # to
]


class AdFixEndDateSettings(JobSettings):
    # Currently only used for calling `.start_logging_based_on_settings` in `cli`

    class Config:
        settings_json_prefix = "integrations.ad.write"


class MOEngagementDateSource:
    """Fetch engagement dates from MO for a given MO user UUID, and return either:
    - a single end date (`get_end_date`), or
    - a current and a future end date (`get_split_end_dates`)
    """

    def __init__(self, graphql_session: SyncClientSession):
        self._graphql_session: SyncClientSession = graphql_session

    @retry(
        wait=wait_fixed(5),
        reraise=True,
        stop=stop_after_delay(10 * 60),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def get_employee_engagement_dates(self, uuid: str) -> list[dict]:
        """Fetch all MO engagement validities for a given MO user UUID"""

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

    def get_end_date(self, uuid: str) -> datetime.datetime | PositiveInfinity | Unset:
        """Return a single engagement end date for a given MO user UUID"""

        try:
            engagements = self.get_employee_engagement_dates(uuid)
        except KeyError:
            return Unset()

        parsed_validities = self._parse_validities(engagements)
        if parsed_validities:
            end_date: datetime.datetime | PositiveInfinity = max(
                (validity[1] for validity in parsed_validities),
                key=lambda end_datetime: self._fold_in_utc(
                    end_datetime, datetime.datetime.max
                ),
            )
            return end_date
        else:
            return Unset()

    def get_split_end_dates(
        self, uuid: str
    ) -> tuple[
        datetime.datetime | PositiveInfinity | Unset,
        datetime.datetime | PositiveInfinity | Unset,
    ]:
        """Return a 2-tuple of (current, future) engagement end dates for a given MO
        user UUID.
        """

        def _split(validity):
            """Split the set of engagement validities into two partitions:
            - one partition of past and present engagements.
            - one partition of future engagements.
            The split is determined by looking at the "from" part of the validity.
            """
            local_date = in_default_tz(datetime.datetime.now()).date()
            # Take either engagement start date, or "negative infinity" if blank
            start_date = self._fold_in_utc(validity[0], datetime.datetime.min).date()
            return start_date > local_date

        try:
            engagements = self.get_employee_engagement_dates(uuid)
        except KeyError:
            # If MO user has no engagements, we should not update neither the current
            # nor the future end date.
            return Unset(), Unset()

        parsed_validities: list[ValidityTuple] = self._parse_validities(engagements)

        # Split by whether the engagement *starts* in the future, or not
        current: Iterator[ValidityTuple]
        future: Iterator[ValidityTuple]
        current, future = partition(_split, parsed_validities)

        # Sort by validity end - the validity with the latest end appears first in list
        current_list: list[ValidityTuple] = sorted(
            current,
            key=lambda validity: self._fold_in_utc(validity[1], datetime.datetime.max),
            reverse=True,
        )

        # Sort by validity start - the validity with the earliest start appears first in
        # list.
        future_list: list[ValidityTuple] = sorted(
            future,
            key=lambda validity: self._fold_in_utc(validity[0], datetime.datetime.min),
            reverse=False,
        )

        # Check if either `current` or `future` (or both) is a 0-length list, and return
        # results accordingly.
        if current_list and future_list:
            # Return end date of the latest current validity and the earliest future
            # validity.
            return current_list[0][1], future_list[0][1]
        elif current_list and not future_list:
            # Return only end date of the latest current validity
            return current_list[0][1], Unset()
        elif not current_list and future_list:
            # Return only end date of the earliest future validity
            return Unset(), future_list[0][1]

        return Unset(), Unset()  # no engagements at all

    def _fold_in_utc(
        self,
        val: datetime.datetime | None | PositiveInfinity | NegativeInfinity,
        default: datetime.datetime,
    ) -> datetime.datetime:
        # Fold in UTC as `datetime.datetime.max` and `datetime.datetime.min` cannot be
        # converted into other timezones without surprising issues.
        return (
            default
            if val in (None, PositiveInfinity(), NegativeInfinity())
            else val.astimezone(datetime.timezone.utc).replace(tzinfo=None)  # type: ignore
        )

    def _parse_validities(self, engagements) -> list[ValidityTuple]:
        """Convert validities from string values to 2-tuple of `datetime.datetime`
        objects.
        """

        def _from_iso_or_none(val: str, none: PositiveInfinity | NegativeInfinity):
            if val:
                maybe_naive_dt = datetime.datetime.fromisoformat(val)
                return in_default_tz(maybe_naive_dt)
            # If val is None or '', convert to the `none` value provided by the caller,
            # either `PositiveInfinity` or `NegativeInfinity`.
            return none

        def _start_from_iso_or_none(val: str) -> datetime.datetime | NegativeInfinity:
            return _from_iso_or_none(val, NegativeInfinity())

        def _end_from_iso_or_none(val: str) -> datetime.datetime | PositiveInfinity:
            return _from_iso_or_none(val, PositiveInfinity())

        def _parse(validity: dict) -> ValidityTuple:
            start = _start_from_iso_or_none(validity["from"])
            end = _end_from_iso_or_none(validity["to"])

            # Sanity check that engagement start is indeed before engagement end
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
    """Represents an existing AD end date."""

    mo_uuid: str
    field_name: str
    field_value: str | None | Invalid

    @property
    def normalized_value(self) -> datetime.datetime | None | Invalid:
        if self.field_value == Invalid():
            return Invalid()
        try:
            return in_default_tz(datetime.datetime.fromisoformat(self.field_value))  # type: ignore
        except (TypeError, ValueError):
            logger.debug("cannot parse %r as ISO datetime", self.field_value)
            return Invalid()


class ADEndDateSource:
    """Fetch AD end date(s) for one or all AD users.

    In case we are updating more than one AD end date (`enddate_field_future` is set),
    this class will yield two `ADUserEndDate` instances per AD user.

    In case we are only updating one AD end date  (`enddate_field_future` is None),
    this class will yield one `ADUserEndDate` instances per AD user.
    """

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

        # This holds the "raw" result of calling either `ADParameterReader.read_it_all`
        # (in `of_all_users`), or `ADParameterReader.read_user` (in `of_one_user`.)
        self._ad_users: list[dict] = []

    def __iter__(self) -> Iterator[ADUserEndDate]:
        """Yield one or two `ADUserEndDate` instances for each AD user in
        `self._ad_users`.

        Iterating over an `ADEndDateSource` only makes sense after calling either
        `of_all_users` or `of_one_user`.
        """
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

    def of_all_users(self) -> Self:
        """Return `ADEndDateSource` for all AD users"""
        self._ad_users = self._reader.read_it_all()
        return self

    def of_one_user(self, username: str) -> Self:
        """Return `ADEndDateSource` for a single AD user given by its AD `username`"""
        self._ad_users = [self._reader.read_user(user=username)]
        return self

    def _get_case_insensitive(self, ad_user: dict, field_name: str) -> str | Invalid:
        # There may be case inconsistencies in AD field names between the name we ask
        # for, and the name that is actually returned. E.g. asking for a field name in
        # all lowercase may return the same field but with a mixed-case name.
        # This method handles this by treating both field names as lowercase.
        #
        # If the field name is not present in the given AD user, an `Invalid` instance
        # is returned.
        try:
            return one(
                (v for k, v in ad_user.items() if k.lower() == field_name.lower())
            )
        except ValueError:
            return Invalid()


class CompareEndDate:
    """Compares the end dates in `mo_engagement_date_source` and
    `ad_end_date_source`, and produces a list of changes that need to be made in AD
    (`get_changes`.)
    """

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

    def get_results(
        self,
    ) -> Iterator[tuple[ADUserEndDate, datetime.datetime | Unset | PositiveInfinity]]:
        """For each AD user end date in `self._ad_end_date_source`, produce the relevant
        MO end date(s).
        """
        for ad_user in self._ad_end_date_source:
            if self._enddate_field_future:
                # Mode two: we are updating a "current" and "future" end date in AD
                split = self._mo_engagement_date_source.get_split_end_dates(
                    ad_user.mo_uuid
                )
                current_mo_end_date, future_mo_end_date = split
                if ad_user.field_name == self._enddate_field:
                    yield ad_user, current_mo_end_date
                if ad_user.field_name == self._enddate_field_future:
                    yield ad_user, future_mo_end_date
            else:
                # Mode one: we are updating a single end date in AD
                end_date = self._mo_engagement_date_source.get_end_date(ad_user.mo_uuid)
                if ad_user.field_name == self._enddate_field:
                    yield ad_user, end_date

    def get_changes(self) -> Iterator[tuple[ADUserEndDate, datetime.datetime]]:
        """For each pair of (AD user end date, MO end date), compare the two, and
        produce a "hit" if the AD user end date differs from the MO end date, and thus
        needs to be updated.
        """
        for ad_user, mo_value in self.get_results():
            ad_value = ad_user.normalized_value

            # Convert `None` to "max date"
            if mo_value is None:
                mo_value = PositiveInfinity().as_datetime()

            # Convert `PositiveInfinity` and `NegativeInfinity` to their respective
            # `datetime.datetime` values.
            if mo_value in (PositiveInfinity(), NegativeInfinity()):
                mo_value = mo_value.as_datetime()  # type: ignore

            if mo_value == Unset():
                # If MO value is `Unset`, leave it out of the set of changes
                logger.info(
                    "MO user %r: skipping %r update as MO value is unset",
                    ad_user.mo_uuid,
                    ad_user.field_name,
                )
            elif ad_value != mo_value:
                # If MO and AD values differ, include in the list of changes
                yield ad_user, mo_value  # type: ignore
            else:
                # MO and AD values must be equal
                logger.debug(
                    "MO user %r: normalized MO and AD values are identical in field %r "
                    "(values in MO: %r, AD: %r)",
                    ad_user.mo_uuid,
                    ad_user.field_name,
                    mo_value.strftime("%Y-%m-%d"),  # type: ignore
                    ad_user.field_value,
                )


class UpdateEndDate(AD):
    """Given a list of changes, perform one or two update commands against AD, to bring
    the end dates of the AD users up-to-date.
    """

    def __init__(self, settings=None):
        super().__init__(all_settings=settings)

    def get_update_cmd(
        self,
        uuid_field: str,
        uuid: str,
        end_date_field: str,
        end_date: str,
    ) -> str:
        """Return the relevant Powershell update command to update the given end date
        field in AD with the given value."""

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
        """Run a PowerShell command against AD"""
        return self._run_ps_script("%s\n%s" % (self._build_user_credential(), cmd))

    def run_all(
        self,
        changes: Iterable[tuple[ADUserEndDate, datetime.datetime]],
        uuid_field: str,
        dry: bool = False,
    ) -> list[tuple[str, Any]]:
        """Take the output of `CompareEndDate.get_changes` and turn it into update
        commands. Run the update commands against AD if `dry` is False, or else just log
        the commands that would have been run.
        """

        changes = tqdm(list(changes))
        num_changes = 0
        retval = []

        for ad_user, mo_value in changes:
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
            logger.debug(cmd)
            if dry:
                retval.append((cmd, "<dry run>"))
            else:
                result = self.run(cmd)
                retval.append((cmd, result))  # type: ignore
                if result != {}:
                    logger.error("AD error response %r", result)
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
    mora_base: str,
    client_id: str,
    client_secret: str,
    auth_realm: str,
    auth_server: str,
):
    """Writes MO engagements end date(s) to one or more fields on the corresponding AD
    user(s).
    """
    pydantic_settings = AdFixEndDateSettings()
    pydantic_settings.start_logging_based_on_settings()

    if pydantic_settings.sentry_dsn:
        sentry_sdk.init(dsn=pydantic_settings.sentry_dsn)

    logger.info(
        f"Command line args:"
        f" end-date-field = {enddate_field},"
        f" end-date-field-future = {enddate_field_future},"
        f" uuid-field = {uuid_field},"
        f" dry-run = {dry_run},"
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
        update.run_all(compare.get_changes(), uuid_field, dry=dry_run)


if __name__ == "__main__":
    cli()

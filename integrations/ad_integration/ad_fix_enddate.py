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
from integrations.ad_integration.ad_template_engine import render_update_by_mo_uuid_cmd


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


class AdFixEndDateSettings(JobSettings):
    # Currently only used for calling `.start_logging_based_on_settings` in `cli`

    class Config:
        settings_json_prefix = "integrations.ad.write"


@dataclass
class ADDate:
    """Represents a date found on an AD user"""

    field_name: str
    field_value: str | None | Invalid

    @property
    def normalized_value(self) -> datetime.datetime | None | Invalid:
        if self.field_value == Invalid():
            return Invalid()
        try:
            return in_default_tz(datetime.datetime.fromisoformat(self.field_value))  # type: ignore
        except (TypeError, ValueError):
            logger.debug(
                "%s: cannot parse %r as ISO datetime",
                self.field_name,
                self.field_value,
            )
            return Invalid()


@dataclass
class ADText:
    """Represents a text field found on an AD user"""

    field_name: str
    field_value: str | None | Invalid


@dataclass
class ADUser:
    """Represents an AD user and its associated data"""

    mo_uuid: str
    end_date: ADDate
    end_date_future: ADDate | None = None
    start_date_future: ADDate | None = None
    org_unit_path: ADText | None = None


class ADUserSource:
    """Fetch AD data for one or all AD users.
    Iterating over an `ADUserSource` instance produces one or more `ADUser` instances.
    """

    def __init__(
        self,
        uuid_field: str,
        enddate_field: str,
        enddate_field_future: str | None,
        startdate_field_future: str | None,
        org_unit_path_field_future: str | None,
        settings: dict | None = None,
    ):
        self._uuid_field = uuid_field
        self._enddate_field = enddate_field
        self._enddate_field_future = enddate_field_future
        self._startdate_field_future = startdate_field_future
        self._org_unit_path_field_future = org_unit_path_field_future

        self._reader = ADParameterReader(all_settings=settings)

        # This holds the "raw" result of calling either `ADParameterReader.read_it_all`
        # (in `of_all_users`), or `ADParameterReader.read_user` (in `of_one_user`.)
        self._ad_users: list[dict] = []

    def __iter__(self) -> Iterator[ADUser]:
        """Yield an `ADUser` instance for each AD user in `self._ad_users`.
        Iterating over an `ADUserSource` only makes sense after calling either
        `of_all_users` or `of_one_user`.
        """
        for ad_user in self._ad_users:
            if self._uuid_field not in ad_user:
                click.echo(
                    f"User with {ad_user['ObjectGuid']=} does not have an "
                    f"{self._uuid_field} field, and will be skipped"
                )
                continue

            yield ADUser(
                ad_user[self._uuid_field],
                self._get_ad_date(ad_user, self._enddate_field),  # type: ignore
                self._get_ad_date(ad_user, self._enddate_field_future),
                self._get_ad_date(ad_user, self._startdate_field_future),
                self._get_ad_text(ad_user, self._org_unit_path_field_future),
            )

    def of_all_users(self) -> Self:
        """Return `ADUserSource` for all AD users"""
        self._ad_users = self._reader.read_it_all()
        return self

    def of_one_user(self, username: str) -> Self:
        """Return `ADUserSource` for a single AD user given by its AD `username`"""
        self._ad_users = [self._reader.read_user(user=username)]
        return self

    def _get_ad_date(self, ad_user: dict, field_name: str | None) -> ADDate | None:
        if field_name is not None:
            return ADDate(field_name, self._get_case_insensitive(ad_user, field_name))
        return None

    def _get_ad_text(self, ad_user: dict, field_name: str | None) -> ADText | None:
        if field_name is not None:
            return ADText(field_name, self._get_case_insensitive(ad_user, field_name))
        return None

    def _get_case_insensitive(
        self, ad_user: dict, field_name: str
    ) -> str | Invalid | None:
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


@dataclass
class MOSimpleEngagement:
    """Represents the MO data to consider in the "simple" case, where we are only
    updating a single end date in AD.

    This class only supports comparisons on the `end_date` field.
    """

    ad_user: ADUser
    end_date: datetime.datetime | PositiveInfinity | Unset

    @property
    def changes(self) -> dict[str, str]:
        """Return a dictionary where the keys are AD field names and the values are
        strings, e.g. the contents are suitable for passing to
        `ChangeListExecutor.get_update_cmd`.
        """
        result: dict[str, str] = {}
        new_end_date: datetime.datetime | None
        new_end_date = self._compare_end_date(self.ad_user.end_date, self.end_date)
        result = self._add_changed_date(self.ad_user.end_date, new_end_date, result)
        return result

    def _compare_end_date(
        self,
        ad_date: ADDate | None,
        mo_date: datetime.datetime | PositiveInfinity | Unset | None,
    ) -> datetime.datetime | None:
        # Convert `None` to "max date"
        if mo_date is None:
            mo_date = PositiveInfinity().as_datetime()
        return self._compare_date(ad_date, mo_date)

    def _compare_date(
        self,
        ad_date: ADDate | None,
        mo_value: datetime.datetime | PositiveInfinity | NegativeInfinity | Unset,
    ) -> datetime.datetime | None:
        # Parse AD date to `datetime.datetime` if available, else use None as its value
        ad_value = ad_date.normalized_value if ad_date else None

        # Convert `PositiveInfinity` and `NegativeInfinity` to their respective
        # `datetime.datetime` values.
        if mo_value in (PositiveInfinity(), NegativeInfinity()):
            mo_value = mo_value.as_datetime()  # type: ignore

        if mo_value == Unset():
            # If MO value is `Unset`, leave it out of the set of changes
            logger.info(
                "MO user %r: skipping %r update as MO value is unset",
                self.ad_user.mo_uuid,
                ad_date.field_name if ad_date is not None else "",
            )
            return None
        elif ad_value != mo_value:
            # If MO and AD values differ, include in the list of changes
            return mo_value  # type: ignore
        else:
            # MO and AD values must be equal
            logger.debug(
                "MO user %r: normalized MO and AD values are identical in field %r "
                "(values in MO: %r, AD: %r)",
                self.ad_user.mo_uuid,
                ad_date.field_name if ad_date is not None else None,
                mo_value.strftime("%Y-%m-%d"),  # type: ignore
                ad_date.field_value if ad_date is not None else None,
            )
            return None

    def _add_changed_date(
        self,
        ad_date: ADDate | None,
        new_value: datetime.datetime | None,
        changes: dict,
    ) -> dict:
        if ad_date is not None and new_value is not None:
            changes[ad_date.field_name] = new_value.strftime("%Y-%m-%d")
        return changes


@dataclass
class MOSplitEngagement(MOSimpleEngagement):
    """Represents the MO data to consider in the complex case, where we look at both the
    current/past engagement, as well as a possible future engagement.

    This class extends `MOSimpleEngagement` to support comparisons on the additional
    fields `end_date_future`, `start_date_future`, and `org_unit_path_future`.
    """

    end_date_future: datetime.datetime | PositiveInfinity | Unset
    start_date_future: datetime.datetime | NegativeInfinity | Unset
    org_unit_path_future: str | Unset

    @property
    def changes(self) -> dict[str, str]:
        # Compare end_date
        result: dict[str, str] = super().changes or {}

        # Compare end_date_future
        new_end_date_future: datetime.datetime | None
        new_end_date_future = self._compare_end_date(
            self.ad_user.end_date_future,
            self.end_date_future,
        )
        result = self._add_changed_date(
            self.ad_user.end_date_future,
            new_end_date_future,
            result,
        )

        # Compare start_date_future
        new_start_date_future: datetime.datetime | None
        new_start_date_future = self._compare_start_date(
            self.ad_user.start_date_future,
            self.start_date_future,
        )
        result = self._add_changed_date(
            self.ad_user.start_date_future,
            new_start_date_future,
            result,
        )

        # Compare org_unit_path_future
        new_org_unit_path_future: str | None
        new_org_unit_path_future = self._compare_org_unit_path(
            self.ad_user.org_unit_path,
            self.org_unit_path_future,
        )
        result = self._add_changed_text(
            self.ad_user.org_unit_path,
            new_org_unit_path_future,
            result,
        )

        return result

    def _compare_start_date(
        self,
        ad_date: ADDate | None,
        mo_date: datetime.datetime | NegativeInfinity | Unset | None,
    ) -> datetime.datetime | None:
        # Convert `None` to "min date"
        if mo_date is None:
            mo_date = NegativeInfinity().as_datetime()
        return self._compare_date(ad_date, mo_date)

    def _compare_org_unit_path(self, ad_text: ADText | None, mo_value) -> str | None:
        ad_value: str | None | Invalid = (
            ad_text.field_value if ad_text is not None else None
        )
        if mo_value == Unset():
            logger.info(
                "MO user %r: skipping %r update as MO value is unset",
                self.ad_user.mo_uuid,
                ad_text.field_name if ad_text is not None else "",
            )
            return None
        elif ad_value != mo_value:
            # If MO and AD values differ, include in the list of changes
            return mo_value
        else:
            # MO and AD values must be equal
            logger.debug(
                "MO user %r: normalized MO and AD values are identical in field %r "
                "(values in MO: %r, AD: %r)",
                self.ad_user.mo_uuid,
                ad_text.field_name if ad_text is not None else None,
                mo_value,
                ad_text.field_value if ad_text is not None else None,
            )
            return None

    def _add_changed_text(
        self,
        ad_text: ADText | None,
        new_value: str | None,
        changes: dict,
    ) -> dict:
        if ad_text is not None and new_value is not None:
            changes[ad_text.field_name] = new_value
        return changes


@dataclass
class _ParsedEngagement:
    # Only used internally by `MOEngagementSource`

    from_dt: datetime.datetime | NegativeInfinity
    to_dt: datetime.datetime | PositiveInfinity
    org_unit: list[dict]

    def get_org_unit_path(self, sep: str = "\\") -> str:
        if len(self.org_unit) > 1:
            logger.warning(
                "more than one `org_unit` found in %r, taking first",
                self.org_unit,
            )
        path = self.org_unit[0]
        return sep.join(
            elem["name"] for elem in (path["ancestors_validity"][::-1] + [path])
        )


class MOEngagementSource:
    """Fetch engagement data from MO for a given MO user UUID, and return either:
    - a `MOSimpleEngagement` (via `get_simple_engagement`), or
    - a `MOSplitEngagement` (via `get_split_engagement`.)
    """

    def __init__(
        self,
        graphql_session: SyncClientSession,
        split: bool = False,
    ):
        # self._enddate_field = enddate_field
        # self._enddate_field_future = enddate_field_future
        self.split = split
        self._graphql_session: SyncClientSession = graphql_session

    def __getitem__(self, ad_user: ADUser):
        if self.split:
            return self.get_split_engagement(ad_user)
        else:
            return self.get_simple_engagement(ad_user)

    @retry(
        wait=wait_fixed(5),
        reraise=True,
        stop=stop_after_delay(10 * 60),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    def _get_employee_engagements(self, uuid: str) -> list[dict]:
        """Fetch all MO engagement validity periods and org unit paths for a given MO
        user UUID."""

        query = gql(
            """
            query Get_mo_engagements($employees: [UUID!]) {
                engagements(employees: $employees, from_date: null, to_date: null) {
                    objects {
                        validity {
                            from
                            to
                        }
                        org_unit {
                            name
                            ancestors_validity {
                                name
                            }
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

    def get_simple_engagement(self, ad_user: ADUser) -> MOSimpleEngagement:
        """Return a `MOSimpleEngagement` for a given MO user UUID"""

        try:
            engagements = self._get_employee_engagements(ad_user.mo_uuid)
        except KeyError:
            return MOSimpleEngagement(ad_user, Unset())

        parsed_engagements = self._parse_engagements(engagements)
        if parsed_engagements:
            end_date: datetime.datetime | PositiveInfinity = max(
                (parsed_engagement.to_dt for parsed_engagement in parsed_engagements),
                key=lambda end_datetime: self._fold_in_utc(
                    end_datetime, datetime.datetime.max
                ),
            )
            return MOSimpleEngagement(ad_user, end_date)
        else:
            return MOSimpleEngagement(ad_user, Unset())

    def get_split_engagement(self, ad_user: ADUser) -> MOSplitEngagement:
        """Return a `MOSplitEngagement` for a given MO user UUID"""

        def _split(parsed_engagement: _ParsedEngagement):
            """Split the set of engagement validities into two partitions:
            - one partition of past and present engagements.
            - one partition of future engagements.
            The split is determined by looking at the "from" part of the validity.
            """
            local_date = in_default_tz(datetime.datetime.now()).date()
            # Take either engagement start date, or "negative infinity" if blank
            start_date = self._fold_in_utc(
                parsed_engagement.from_dt, datetime.datetime.min
            ).date()
            return start_date > local_date

        try:
            engagements = self._get_employee_engagements(ad_user.mo_uuid)
        except KeyError:
            # If MO user has no engagements, we should not update neither the current
            # nor the future end date.
            return MOSplitEngagement(ad_user, Unset(), Unset(), Unset(), Unset())

        parsed_engagements: list[_ParsedEngagement] = self._parse_engagements(
            engagements
        )

        # Split by whether the engagement *starts* in the future, or not
        current: Iterator[_ParsedEngagement]
        future: Iterator[_ParsedEngagement]
        current, future = partition(_split, parsed_engagements)

        # Sort by validity end - the validity with the latest end appears first in list
        current_list: list[_ParsedEngagement] = sorted(
            current,
            key=lambda parsed_engagement: self._fold_in_utc(
                parsed_engagement.to_dt,
                datetime.datetime.max,
            ),
            reverse=True,
        )

        # Sort by validity start - the validity with the earliest start appears first in
        # list.
        future_list: list[_ParsedEngagement] = sorted(
            future,
            key=lambda parsed_engagement: self._fold_in_utc(
                parsed_engagement.from_dt,
                datetime.datetime.min,
            ),
            reverse=False,
        )

        # Check if either `current` or `future` (or both) is a 0-length list, and return
        # results accordingly.
        if current_list and future_list:
            # Return end date of the latest current validity and the earliest future
            # validity.
            return MOSplitEngagement(
                ad_user,
                current_list[0].to_dt,  # end_date
                future_list[0].to_dt,  # end_date_future
                future_list[0].from_dt,  # start_date_future
                future_list[0].get_org_unit_path(),  # org_unit_path_future
            )
        elif current_list and not future_list:
            # Return only end date of the latest current validity
            return MOSplitEngagement(
                ad_user,
                current_list[0].to_dt,  # end_date
                Unset(),  # end_date_future
                Unset(),  # start_date_future
                Unset(),  # org_unit_path_future
            )
        elif not current_list and future_list:
            # Return only end date of the earliest future validity
            return MOSplitEngagement(
                ad_user,
                Unset(),  # end_date
                future_list[0].to_dt,  # end_date_future
                future_list[0].from_dt,  # start_date_future
                future_list[0].get_org_unit_path(),  # org_unit_path_future
            )

        # No engagements at all
        return MOSplitEngagement(ad_user, Unset(), Unset(), Unset(), Unset())

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

    def _parse_engagements(self, engagements) -> list[_ParsedEngagement]:
        """Convert validities from string values to 2-tuple of `datetime.datetime`
        objects.
        """

        def _from_iso_or_none(val: str, none: PositiveInfinity | NegativeInfinity):
            if val:
                maybe_naive_dt = datetime.datetime.fromisoformat(val)
                if maybe_naive_dt.tzinfo:
                    return maybe_naive_dt
                else:
                    return in_default_tz(maybe_naive_dt)
            # If val is None or '', convert to the `none` value provided by the caller,
            # either `PositiveInfinity` or `NegativeInfinity`.
            return none

        def _start_from_iso_or_none(val: str) -> datetime.datetime | NegativeInfinity:
            return _from_iso_or_none(val, NegativeInfinity())

        def _end_from_iso_or_none(val: str) -> datetime.datetime | PositiveInfinity:
            return _from_iso_or_none(val, PositiveInfinity())

        def _parse(engagement: dict) -> _ParsedEngagement:
            validity = engagement["validity"]
            org_unit = engagement["org_unit"]

            start = _start_from_iso_or_none(validity["from"])
            end = _end_from_iso_or_none(validity["to"])

            # Sanity check that engagement start is indeed before engagement end
            folded_start = self._fold_in_utc(start, datetime.datetime.min)
            folded_end = self._fold_in_utc(end, datetime.datetime.max)
            assert folded_start <= folded_end

            return _ParsedEngagement(start, end, org_unit)

        parsed_validities = [
            _parse(obj) for engagement in engagements for obj in engagement["objects"]
        ]

        return parsed_validities


class ChangeList:
    """Compares the data in `mo_engagement_source` and `ad_user_source`, and produces a
    list of changes that need to be made in AD.
    """

    def __init__(
        self,
        mo_engagement_source: MOEngagementSource,
        ad_user_source: ADUserSource,
    ):
        self._mo_engagement_source = mo_engagement_source
        self._ad_user_source = ad_user_source

    def get_changes(self) -> Iterator[MOSimpleEngagement | MOSplitEngagement]:
        """For each AD user end date in `self._ad_user_source`, produce the relevant
        MO data (either a `MOSimpleEngagement` or a `MOSplitEngagement`.)
        """
        for ad_user in self._ad_user_source:
            match: MOSimpleEngagement | MOSplitEngagement | None
            match = self._mo_engagement_source[ad_user]
            if match is not None and match.changes:
                yield match


class ChangeListExecutor(AD):
    """Given a list of changes, perform update commands against AD to bring the data of
    the AD users up-to-date with the corresponding MO users.
    """

    def __init__(self, settings=None):
        super().__init__(all_settings=settings)

    def get_update_cmd(
        self,
        uuid_field: str,
        uuid: str,
        **changes,
    ) -> str:
        """Return the relevant Powershell update command to update the given end date
        field in AD with the given value."""
        return self.remove_redundant(
            render_update_by_mo_uuid_cmd(
                self._ps_boiler_plate()["complete"],
                self._ps_boiler_plate()["credentials"],
                uuid_field,
                uuid,
                changes,
            )
        )

    def run(self, cmd) -> dict:
        """Run a PowerShell command against AD"""
        return self._run_ps_script("%s\n%s" % (self._build_user_credential(), cmd))

    def run_all(
        self,
        changes: Iterable[MOSimpleEngagement | MOSplitEngagement],
        uuid_field: str,
        dry: bool = False,
    ) -> list[tuple[str, Any]]:
        """Take the output of `ChangeList.get_changes` and turn it into update
        commands. Run the update commands against AD if `dry` is False, or else just log
        the commands that would have been run.
        """

        changes = tqdm(list(changes))
        num_changes = 0
        retval = []

        for change in changes:
            if change.changes == {}:
                logger.debug("skip unchanged user %r", change.ad_user)
                continue
            cmd = self.get_update_cmd(
                uuid_field,
                change.ad_user.mo_uuid,
                **change.changes,
            )
            logger.info(
                "Updating AD user %r: %r",
                change.ad_user.mo_uuid,
                change.changes,
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
@click.option(
    "--startdate-field-future",
    default=load_setting("integrations.ad_writer.fixup_future_startdate_field", None),
)
@click.option(
    "--orgunitpath-field-future",
    default=load_setting("integrations.ad_writer.fixup_future_orgunitpath_field", None),
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
    startdate_field_future,
    orgunitpath_field_future,
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
        f" start-date-field-future = {startdate_field_future},"
        f" org-unit-path-field-future = {orgunitpath_field_future},"
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

    ad_user_source = ADUserSource(
        uuid_field,
        enddate_field,
        enddate_field_future,
        startdate_field_future,
        orgunitpath_field_future,
    )
    if ad_user:
        ad_user_source = ad_user_source.of_one_user(ad_user)
    else:
        ad_user_source = ad_user_source.of_all_users()

    with graphql_client as session:
        mo_engagement_source = MOEngagementSource(
            session,
            split=True if enddate_field_future else False,
        )
        change_list = ChangeList(mo_engagement_source, ad_user_source)
        executor = ChangeListExecutor()
        executor.run_all(change_list.get_changes(), uuid_field, dry=dry_run)


if __name__ == "__main__":
    cli()

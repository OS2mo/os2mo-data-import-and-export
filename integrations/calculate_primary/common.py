import datetime
import logging
from abc import ABC
from abc import abstractmethod
from functools import lru_cache
from functools import partial
from operator import itemgetter
from typing import Union
from uuid import UUID

from more_itertools import ilen
from more_itertools import only
from more_itertools import pairwise
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.deprecation import deprecated
from ra_utils.load_settings import load_settings
from ra_utils.tqdm_wrapper import tqdm


LOGGER_NAME = "updatePrimaryEngagements"
logger = logging.getLogger(LOGGER_NAME)


class MultipleFixedPrimaries(Exception):
    """Thrown when multiple fixed primaries are found doing recalculate.

    This means that a user entered invalid data in MO.
    """

    pass


class NoPrimaryFound(Exception):
    """Thrown when no primary is determined doing recalculate.

    This means the implementation specific backend did not fulfill the interface for
    the _find_primary method.
    """

    pass


def noop(*args, **kwargs):
    """Noop function, which consumes arguments and does nothing."""
    pass


class MOPrimaryEngagementUpdater(ABC):
    def __init__(self, settings=None, dry_run=False):
        self.settings = settings or load_settings()
        self.dry_run = dry_run

        self.helper = self._get_mora_helper(self.settings["mora.base"])

        # List of engagement filters to apply to check / recalculate respectively
        # NOTE: Should be overridden by subclasses
        self.check_filters = []
        self.calculate_filters = []

        self.primary_types, self.primary = self._find_primary_types()

    def _get_mora_helper(self, mora_base):
        """Construct a MoraHelper object.

        Split out solely to ease testing.
        """
        return MoraHelper(hostname=mora_base, use_cache=False)

    def _get_person(self, cpr=None, uuid=None, mo_person=None):
        """Fetch a person from MO.

        Only one of the given parameters should be given, if multiple are given
        UUID takes priority over CPR which in turn takes priority over mo_person.

        Args:
            cpr: The CPR number of the person.
            uuid: The MO uuid of the person.
            mo_person: An existing user object from MoraHelper.

        Returns:
            user object from MoraHelper.
        """

        @lru_cache(maxsize=None)
        def _get_org_uuid():
            org_uuid = self.helper.read_organisation()
            return org_uuid

        if uuid:
            mo_person = self.helper.read_user(user_uuid=uuid)
        elif cpr:
            mo_person = self.helper.read_user(
                user_cpr=cpr, org_uuid=self._get_org_uuid()
            )
        return mo_person

    def _read_engagement(self, user_uuid, date):
        """Fetch all engagements for user_uuid at date."""
        mo_engagements = self.helper.read_user_engagements(
            user=user_uuid,
            at=date,
            only_primary=True,  # Do not read extended info from MO.
            use_cache=False,
        )
        return mo_engagements

    @abstractmethod
    def _find_primary_types(self):
        """Find primary classes for the underlying implementation.

        Returns:
            2-tuple:
                primary_types: a dict from indirect primary names to UUIDs.
                    The used names are 'fixed_primary', 'primary' and 'non_primary',
                    as such these names should be keys in the dictionary.
                primary: a list of UUIDs that can considered to be primary.
                    Should be a subset of the values in primary_types.
        """
        raise NotImplementedError()

    @abstractmethod
    def _find_primary(self, mo_engagements):
        """Decide which of the engagements in mo_engagements is the primary one.

        This method does not need to handle fixed_primaries as the method will
        not be called if fixed_primaries exist.

        Args:
            mo_engagements: List of engagements

        Returns:
            UUID: The UUID of the primary engagement.
        """
        raise NotImplementedError()

    def _predicate_primary_is(self, primary_type_key, engagement):
        """Predicate on an engagements primary type.

        Example:

            is_non_primary = partial(_predicate_primary_is, "non_primary")
            mo_engagement = ...
            is_mo_engagement_non_primary = is_non_primary(mo_engagement)

        Args:
            primary_type_key: Lookup key into self.primary_types
            engagement: The engagement to check primary status from

        Returns:
            boolean: True if engagement has primary type equal to primary_type_key
                     False otherwise
        """
        assert primary_type_key in self.primary_types

        if not engagement.get("primary"):
            return False

        if engagement["primary"]["uuid"] == self.primary_types[primary_type_key]:
            logger.info(
                "Engagement {} is {}".format(engagement["uuid"], primary_type_key)
            )
            return True
        return False

    def _count_primary_engagements(self, check_filters, user_uuid, mo_engagements):
        """Count number of primaries.

        Args:
            check_filters: A list of predicate functions from (user_uuid, eng).
            user_uuid: UUID of the user to who owns the engagements.
            engagements: A list of MO engagements to count primaries from.

        Returns:
            3-tuple:
                engagement_count: Number of engagements processed.
                primary_count: Number of primaries found.
                filtered_primary_count: Number of primaries passing check_filters.
        """
        # Count number of engagements
        mo_engagements = list(mo_engagements)
        engagement_count = len(mo_engagements)

        # Count number of primary engagements, by filtering on self.primary
        primary_mo_engagements = list(
            filter(
                lambda eng: eng["primary"]["uuid"] in self.primary,
                mo_engagements,
            )
        )
        primary_count = len(primary_mo_engagements)

        # Count number of primary engagements, by filtering out special primaries
        # What consistutes a 'special primary' depend on the subclass implementation
        for filter_func in check_filters:
            primary_mo_engagements = filter(
                partial(filter_func, user_uuid), primary_mo_engagements
            )
        filtered_primary_count = ilen(primary_mo_engagements)

        return engagement_count, primary_count, filtered_primary_count

    def _check_user(self, check_filters, user_uuid):
        """Check the users primary engagement(s).

        Args:
            check_filters: A list of predicate functions from (user_uuid, eng).
            user_uuid: UUID of the user to check.

        Returns:
            Dictionary:
                key: Date at which the value is valid.
                value: A 3-tuple, from _count_primary_engagements.
        """
        # List of cut dates, excluding the very last one
        date_list = self.helper.find_cut_dates(uuid=user_uuid)
        date_list = date_list[:-1]
        # Map all our dates, to their corresponding engagements.
        mo_engagements = map(partial(self._read_engagement, user_uuid), date_list)
        # Map mo_engagements to primary counts
        primary_counts = map(
            partial(self._count_primary_engagements, check_filters, user_uuid),
            mo_engagements,
        )
        # Create dicts from cut_dates --> primary_counts
        return dict(zip(date_list, primary_counts))

    def _check_user_outputter(self, check_filters, user_uuid):
        """Check the users primary engagement(s).

        Args:
            check_filters: A list of predicate functions from (user_uuid, eng).
            user_uuid: UUID of the user to check.

        Returns:
            Generator of output 4-tuples:
                outputter: Function to output strings to
                string: The base output string
                user_uuid: User UUID for the output string
                date: Date for the output string
        """

        def to_output(e_count, p_count, fp_count):
            if e_count == 0:
                return (noop, "")
            if p_count == 0:
                return (print, "No primary")
            if p_count == 1:
                return (noop, "")
            if fp_count == 0:
                return (logger.info, "All primaries are special")
            if fp_count == 1:
                return (logger.info, "Only one non-special primary")
            return (print, "Too many primaries")

        user_results = self._check_user(check_filters, user_uuid)
        for date, (e_count, p_count, fp_count) in user_results.items():
            outputter, string = to_output(e_count, p_count, fp_count)
            yield outputter, string, user_uuid, date

    def _check_user_strings(self, check_filters, user_uuid):
        """Check the users primary engagement(s).

        Args:
            check_filters: A list of predicate functions from (user_uuid, eng).
            user_uuid: UUID of the user to check.

        Returns:
            Generator of output 2-tuples:
                outputter: Function to output strings to
                string: Formatted output string
        """
        outputs = self._check_user_outputter(check_filters, user_uuid)
        for outputter, string, user_uuid, date in outputs:
            final_string = string + " for {} at {}".format(user_uuid, date.date())
            yield outputter, final_string

    def check_user(self, user_uuid):
        """Check the users primary engagement(s).

        Prints messages to stdout / log as side-effect.

        Args:
            user_uuid: UUID of the user to check.

        Returns:
            None
        """
        outputs = self._check_user_strings(self.check_filters, user_uuid)
        for outputter, string in outputs:
            outputter(string)

    @deprecated
    def recalculate_primary(self, *args, **kwargs):
        """Deprecated alias for recalculate_user."""
        # Kept solely for backwards compatability
        return self.recalculate_user(*args, **kwargs)

    def _decide_primary(self, mo_engagements):
        """Decide which of the engagements in mo_engagements is the primary one.

        Args:
            mo_engagements: List of engagements

        Returns:
            2-tuple:
                UUID: The UUID of the primary engagement.
                primary_type_key: The type of the primary.
        """
        # First we attempt to find a fixed primary engagement.
        # If multiple are found, we throw an exception, as only one is allowed.
        # If one is found, it is our primary and we are done.
        # If none are found, we need to calculate the primary engagement.
        find_fixed_primary = partial(self._predicate_primary_is, "fixed_primary")

        # Iterator of UUIDs of engagements with primary = fixed_primary
        fixed_primary_engagement_uuids = map(
            itemgetter("uuid"), filter(find_fixed_primary, mo_engagements)
        )
        # UUID of engagement with primary = fixed_primary, exception or None
        fixed = only(
            fixed_primary_engagement_uuids, None, too_long=MultipleFixedPrimaries
        )
        if fixed:
            return fixed, "fixed_primary"

        # No fixed engagements, thus we must calculate the primary engagement.
        #
        # The calulcation of primary engagement depends on the underlying
        # implementation, thus we simply call self._find_primary here.
        primary = self._find_primary(mo_engagements)
        if primary:
            return primary, "primary"
        raise NoPrimaryFound()

    def _ensure_primary(self, engagement, primary_type_uuid, validity):
        """Ensure that engagement has the right primary_type.

        Assuming the engagement already has the correct primary_type this method
        is a noop, wheres if this is not the case an update request will be made
        against MO.

        Args:
            engagement: The engagement to (potentially) update.
            primary_type_uuid: The primary type to ensure the engagement has.
            validity: The validity of the change (if made).

        Returns:
            boolean: True if a change is made, False otherwise.
        """
        # Check if the required primary type is already set
        if engagement["primary"]["uuid"] == primary_type_uuid:
            logger.info(
                "No update as primary type is not changed: {}".format(validity["from"])
            )
            return False

        # At this point, we know that we have to update the engagement, thus we
        # construct an update payload and send it to MO.
        payload = {
            "type": "engagement",
            "uuid": engagement["uuid"],
            "data": {"primary": {"uuid": primary_type_uuid}, "validity": validity},
        }
        logger.debug("Edit payload: {}".format(payload))

        if not self.dry_run:
            response = self.helper._mo_post("details/edit", payload)
            assert response.status_code in (200, 400)
            if response.status_code == 400:
                # XXX: This shouldn't happen due to the previous check?
                logger.warn("Attempted edit, but no change needed.")
                return False
        return True

    def recalculate_user(self, user_uuid: Union[UUID, str], no_past=False):
        """(Re)calculate primary engagement for the entire history the user."""
        user_uuid = str(user_uuid)

        def fetch_mo_engagements(date):
            """Fetch engagements which are active at 'date' and fulfill our filters.

            Also ensures that the 'primary' attribute is set on all engagements.
            """

            def ensure_primary(engagement):
                """Ensure that engagement has a primary field."""
                # TODO: It would seem this happens for leaves, should we make a
                #       special type for this?
                # TODO: What does the above even mean? - Help?
                if not engagement["primary"]:
                    engagement["primary"] = {"uuid": self.primary_types["non_primary"]}
                return engagement

            # Fetch engagements
            mo_engagements = self._read_engagement(user_uuid, date)
            # Filter unwanted engagements
            for filter_func in self.calculate_filters:
                mo_engagements = filter(
                    partial(filter_func, user_uuid, no_past), mo_engagements
                )
            # Enrich engagements with primary, if required
            mo_engagements = map(ensure_primary, mo_engagements)
            mo_engagements = list(mo_engagements)

            return mo_engagements

        def calculate_validity(start, end):
            """Construct engagement primarity validity from start and end date."""
            to = datetime.datetime.strftime(
                end - datetime.timedelta(days=1), "%Y-%m-%d"
            )
            # Sentinel value for infinity is usually 9999-12-30 / 9999-12-31.
            # We assume anything above 9999-1-1 is sentinel value for infinity.
            if end >= datetime.datetime(9999, 1, 1, 0, 0):
                to = None
            validity = {
                "from": datetime.datetime.strftime(start, "%Y-%m-%d"),
                "to": to,
            }
            return validity

        logger.info("Calculate primary engagement: {}".format(user_uuid))
        number_of_edits = 0

        # Find a list of dates with changes in engagement, and for each change
        # decide which engagement is the primary between that and the next change.
        date_list = self.helper.find_cut_dates(user_uuid, no_past=no_past)
        for start, end in pairwise(date_list):
            logger.info("Recalculate primary, date: {}".format(start))

            mo_engagements = fetch_mo_engagements(start)
            logger.debug("MO engagements: {}".format(mo_engagements))

            # No engagements, no primary, and thus nothing to do
            if len(mo_engagements) == 0:
                continue

            # Decide which of the mo_engagements is the primary one, and also what
            # kind of primary it is, fixed_primary or just primary
            try:
                primary_uuid, primary_type_key = self._decide_primary(mo_engagements)
            except NoPrimaryFound:
                logger.warning(f"Unable to determine primary for {user_uuid}")
                primary_uuid = None

            validity = calculate_validity(start, end)

            # Update the primary type of all engagements (if required)
            for engagement in mo_engagements:
                # As there can only be one primary engagement at the time, all
                # engagements are non_primary by default.
                primary_type_uuid = self.primary_types["non_primary"]
                # Only the primary engagement is marked non_primary. The actual type
                # is simply the one provided by _decide_primary.
                if engagement["uuid"] == primary_uuid:
                    primary_type_uuid = self.primary_types[primary_type_key]

                changed = self._ensure_primary(engagement, primary_type_uuid, validity)
                if changed:
                    number_of_edits += 1

        return_dict = {user_uuid: number_of_edits}
        return return_dict

    def check_all(self):
        """Check all users for the existence of primary engagements."""
        print("Reading all users from MO...")
        all_users = self.helper.read_all_users()
        print("OK")
        for user in tqdm(all_users):
            self.check_user(user["uuid"])

    def recalculate_all(self, no_past=False):
        """Recalculate all users primary engagements."""
        print("Reading all users from MO...")
        all_users = self.helper.read_all_users()
        print("OK")
        edit_status = {}
        all_users = tqdm(all_users)
        all_users = map(itemgetter("uuid"), all_users)
        for user_uuid in all_users:
            try:
                status = self.recalculate_user(user_uuid, no_past=no_past)
                edit_status.update(status)
            except MultipleFixedPrimaries:
                print("{} has conflicting fixed primaries".format(user_uuid))
            except Exception as exp:
                print("Exception while processing {}: {}".format(user_uuid, exp))

        total_non_edits = 0
        total_edits = 0
        for number_of_edits in edit_status.values():
            if number_of_edits == 0:
                total_non_edits += 1
            total_edits += number_of_edits
        print("Total non-edits: {}".format(total_non_edits))
        print("Total edits: {}".format(total_edits))

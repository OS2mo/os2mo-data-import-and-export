import asyncio
import datetime
import logging
from abc import ABC, abstractmethod
from functools import lru_cache, partial
from operator import itemgetter

from exporters.utils.load_settings import load_settings
from more_itertools import ilen, pairwise, flatten, chunked
from os2mo_helpers.mora_helpers import MoraHelper
from tqdm import tqdm
from integrations.dar_helper.utils import async_to_sync

LOGGER_NAME = "updatePrimaryEngagements"
logger = logging.getLogger(LOGGER_NAME)


def asyncio_concurrency(concurrency=10):
    def decorator(function):
        semaphore = asyncio.Semaphore(concurrency)

        async def wrapper(*args, **kwargs):
            async with semaphore:
                return await function(*args, **kwargs)
        return wrapper
    return decorator


def edit_engagement(data, mo_engagement_uuid):
    payload = {"type": "engagement", "uuid": mo_engagement_uuid, "data": data}
    return payload


class MOPrimaryEngagementUpdater(ABC):
    def __init__(self):
        self.settings = load_settings()
        mora_base = self.settings["mora.base"]

        self.helper = MoraHelper(hostname=mora_base, use_cache=False)

        # List of engagement filters to apply to check / recalculate respectively
        # NOTE: Should be overridden by subclasses
        self.check_filters = []
        self.calculate_filters = []

        # self.primary_types is a dict with all classes in the primary facet.
        # self.primary is an ordered list of classes that can considered to be primary.
        self.primary_types, self.primary = self._find_primary_types()

    @lru_cache(maxsize=None)
    def _get_org_uuid(self):
        org_uuid = self.helper.read_organisation()
        return org_uuid

    def _get_person(self, cpr=None, uuid=None, mo_person=None):
        """
        Set a new person as the current user. Either a cpr-number or
        an uuid should be given, not both.
        :param cpr: cpr number of the person.
        :param uuid: MO uuid of the person.
        :param mo_person: An already existing user object from mora_helper.
        """
        if uuid:
            mo_person = self.helper.read_user(user_uuid=uuid)
        elif cpr:
            mo_person = self.helper.read_user(
                user_cpr=cpr, org_uuid=self._get_org_uuid()
            )
        return mo_person

    async def _read_engagement(self, user_uuid, date):
        mo_engagements = await self.helper.a_read_user_engagement(
            user=user_uuid,
            at=date,
            only_primary=True,  # Do not read extended info from MO.
            use_cache=False,
        )
        return mo_engagements

    @abstractmethod
    def _find_primary_types(self):
        raise NotImplementedError

    @abstractmethod
    def _calculate_rate_and_ids(self, mo_engagement, no_past):
        raise NotImplementedError

    @abstractmethod
    def _handle_non_integer_employment_id(self, validity, eng):
        raise NotImplementedError

    @abstractmethod
    def _is_primary(self, employment_id, eng, min_id, impl_specific):
        raise NotImplementedError

    async def check_user(self, user_uuid):
        # List of cut dates, excluding the very last one
        date_list = await self.helper.a_find_cut_dates(uuid=user_uuid)
        date_list = date_list[:-1]
        # Map all our dates, to their corresponding engagements.
        mo_engagement_tasks = list(map(
            partial(self._read_engagement, user_uuid), date_list
        ))
        mo_engagements = list(flatten(await asyncio.gather(*mo_engagement_tasks)))
        # Only keep engagements, which are primary
        primary_mo_engagements = filter(
            lambda eng: eng["engagement_type"]["uuid"] in self.primary,
            mo_engagements,
        )
        # Count number of primary engagements in the iterator
        primary_count = ilen(primary_mo_engagements)

        if primary_count == 0:
            print("No primary for {}".format(user_uuid))
        elif primary_count > 1:
            # Re-count primaries to ensure only one 'true' primary is left
            extra_primary_mo_engagements = mo_engagements
            for filter_func in self.check_filters:
                extra_primary_mo_engagements = filter(
                    partial(filter_func, user_uuid), extra_primary_mo_engagements
                )
            extra_primary_count = ilen(extra_primary_mo_engagements)

            if primary_count == 0:
                logger.info("All primaries are special for {}".format(user_uuid))
            elif primary_count == 1:
                logger.info("Only one non-special primary for {}".format(user_uuid))
            else:
                print("Too many primaries for {} at {}".format(user_uuid))

    def recalculate_primary(self, user_uuid, no_past=False):
        """
        Re-calculate primary engagement for the entire history of the current user.
        """
        logger.info("Calculate primary engagement: {}".format(user_uuid))
        date_list = self.helper.find_cut_dates(user_uuid, no_past=no_past)
        number_of_edits = 0

        for date, next_date in pairwise(date_list):
            logger.info("Recalculate primary, date: {}".format(date))

            # Filter unwanted engagements
            mo_engagements = self._read_engagement(user_uuid, date)
            for filter_func in self.calculate_filters:
                mo_engagements = filter(
                    partial(filter_func, user_uuid, no_past), mo_engagements
                )
            mo_engagements = list(mo_engagements)

            # If no engagements are left, there is no work to do here
            if len(mo_engagements) == 0:
                continue
            logger.debug("MO engagements: {}".format(mo_engagements))

            (min_id, impl_specific) = self._calculate_rate_and_ids(
                mo_engagements, no_past
            )
            if (min_id is None) or (impl_specific is None):
                continue

            # Enrich engagements with primary, if required
            # TODO: It would seem this happens for leaves, should we make a
            #       special type for this?
            # XXX: This should probably not be done as a side-effect!
            for eng in mo_engagements:
                if not eng["primary"]:
                    eng["primary"] = {"uuid": self.primary_types["non_primary"]}

            # XXX: Should we detect and handle multiple fixed primary engagements,
            # or just pick the last one here, and why the last one??
            fixed = None
            for eng in mo_engagements:
                if eng["primary"]["uuid"] == self.primary_types["fixed_primary"]:
                    logger.info("Engagement {} is fixed primary".format(eng["uuid"]))
                    fixed = eng["uuid"]

            exactly_one_primary = False
            for eng in mo_engagements:
                to = datetime.datetime.strftime(
                    next_date - datetime.timedelta(days=1), "%Y-%m-%d"
                )
                if next_date == datetime.datetime(9999, 12, 30, 0, 0):
                    to = None
                validity = {
                    "from": datetime.datetime.strftime(date, "%Y-%m-%d"),
                    "to": to,
                }

                if "user_key" not in eng:
                    break  # Why break instead of continue?!

                try:
                    # non-integer user keys should universally be status0
                    # XXX: So why are they not? - Is this invariant being broken??
                    # What does non-integer mean for OPUS?
                    employment_id = int(eng["user_key"])
                except ValueError:
                    self._handle_non_integer_employment_id()
                    continue

                if self._is_primary(employment_id, eng, min_id, impl_specific):
                    assert exactly_one_primary is False
                    logger.debug("Primary is: {}".format(employment_id))
                    exactly_one_primary = True
                    current_type = self.primary_types["primary"]
                else:
                    logger.debug("{} is not primary".format(employment_id))
                    current_type = self.primary_types["non_primary"]

                if fixed is not None and eng["uuid"] != fixed:
                    # A fixed primary exists, but this is not it.
                    # XXX: Really it could be if multiple fixed exists, it just does
                    #      not happen to be 'the last one' for some ordering.
                    logger.debug("Manual override, this is not primary!")
                    current_type = self.primary_types["non_primary"]
                if eng["uuid"] == fixed:
                    # This is a fixed primary.
                    current_type = self.primary_types["fixed_primary"]

                data = {"primary": {"uuid": current_type}, "validity": validity}

                payload = edit_engagement(data, eng["uuid"])
                if not payload["data"]["primary"] == eng["primary"]:
                    logger.debug("Edit payload: {}".format(payload))
                    response = self.helper._mo_post("details/edit", payload)
                    assert response.status_code in (200, 400)
                    if response.status_code == 400:
                        logger.info("Attempted edit, but no change needed.")
                    number_of_edits += 1
                else:
                    logger.debug("No edit, primary type not changed.")
        return_dict = {user_uuid: number_of_edits}
        return return_dict

    @async_to_sync
    async def check_all(self):
        """
        Check all users for the existence of primary engagements.
        :return: TODO
        """
        print("Reading all users from MO...")
        all_users = await self.helper.a_read_all_users()
        user_count = len(all_users)
        print("OK")
        all_user_uuids = map(itemgetter('uuid'), all_users)

        # Only run 10 check user calls in parallel
        check_user_helper = asyncio_concurrency(50)(self.check_user)
        all_user_check_tasks = map(check_user_helper, all_user_uuids)
        results = asyncio.as_completed(all_user_check_tasks)
        for coro in tqdm(results, total=user_count):
            await coro

    def recalculate_all(self, no_past=False):
        """
        Recalculate all primary engagements
        :return: TODO
        """
        print("Reading all users from MO...")
        all_users = self.helper.read_all_users()
        print("OK")
        edit_status = {}
        for user in tqdm(all_users):
            status = self.recalculate_primary(user["uuid"], no_past=no_past)
            edit_status.update(status)
        print("Total edits: {}".format(sum(edit_status.values())))

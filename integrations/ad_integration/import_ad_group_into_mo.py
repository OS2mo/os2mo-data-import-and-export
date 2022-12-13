import datetime
import logging
from operator import itemgetter
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from uuid import UUID

import click
import sentry_sdk
from click_option_group import optgroup
from click_option_group import RequiredMutuallyExclusiveOptionGroup
from more_itertools import one
from mox_helpers.mox_util import ensure_class_in_lora
from os2mo_helpers.mora_helpers import MoraHelper
from ra_utils.load_settings import load_settings
from ra_utils.tqdm_wrapper import tqdm

import constants
from . import payloads
from .ad_logger import start_logging
from .ad_reader import ADParameterReader


LOG_FILE = "external_ad_users.log"

logger = logging.getLogger("ImportADGroup")


class ADMOImporter(object):
    def __init__(self):
        self.run_date = datetime.datetime.now().strftime("%Y-%m-%d")

        self.settings = self._get_settings()
        self.helper = self._get_mora_helper()

        self.root_ou_uuid = self.settings["integrations.ad.import_ou.mo_unit_uuid"]
        self.org_uuid = self.helper.read_organisation()

        self.AD_it_system_uuid = self._get_ad_it_system()

        self.ad_reader = self._get_ad_reader()

    def _get_settings(self) -> Dict[str, Any]:
        return load_settings()

    def _get_mora_helper(self) -> MoraHelper:
        return MoraHelper(hostname=self.settings["mora.base"], use_cache=False)

    def _get_ad_it_system(self) -> UUID:
        it_systems = self.helper.read_it_systems()
        ad_it_systems: dict = one(
            filter(lambda x: x["name"] == constants.AD_it_system, it_systems)
        )
        return ad_it_systems["uuid"]

    def _get_ad_reader(self) -> ADParameterReader:
        ad_reader = ADParameterReader()
        ad_reader.cache_all(print_progress=True)
        return ad_reader

    def _ensure_class_in_lora(
        self, facet_bvn: str, class_value: str
    ) -> Tuple[UUID, bool]:
        return ensure_class_in_lora(facet_bvn, class_value)

    def _find_or_create_unit_and_classes(self):
        """
        Find uuids of the needed unit and classes for the import. If any unit or
        class is missing from MO, it will be created.The function returns a dict
        containg uuids needed to create users and engagements.
        """
        # TODO: Add a dynamic creation of classes
        job_type, _ = self._ensure_class_in_lora("engagement_job_function", "Ekstern")
        eng_type, _ = self._ensure_class_in_lora("engagement_type", "Ekstern")
        org_unit_type, _ = self._ensure_class_in_lora("org_unit_type", "Ekstern")

        unit = self.helper.read_ou(uuid=self.root_ou_uuid)
        if "status" in unit:  # Unit does not exist
            payload = payloads.create_unit(
                self.root_ou_uuid, "Eksterne Medarbejdere", org_unit_type, self.org_uuid
            )
            logger.debug("Create department payload: {}".format(payload))
            response = self.helper._mo_post("ou/create", payload)
            assert response.status_code == 201
            logger.info("Created unit for external employees")
            logger.debug("Response: {}".format(response.text))

        uuids = {
            "job_function": job_type,
            "engagement_type": eng_type,
            "unit_uuid": self.root_ou_uuid,
        }
        return uuids

    def _find_ou_users_in_ad(self) -> Dict[UUID, List]:
        """
        find users from AD that match a search string in DistinguishedName.
        """
        search_string = self.settings["integrations.ad.import_ou.search_string"]

        def filter_users(user: Dict) -> bool:
            name = user.get("DistinguishedName")
            if name:
                if search_string in name:
                    return True
            return False

        users = list(filter(filter_users, self.ad_reader.results.values()))
        uuids = map(itemgetter("ObjectGUID"), users)
        users_dict = dict(zip(uuids, users))
        return users_dict

    def _create_user(
        self, ad_user: Dict, cpr_field: str, uuid: Optional[str] = None
    ) -> Optional[UUID]:
        """
        Create or update a user in MO using an AD user as a template.
        The user will share uuid between MO and AD.
        :param ad_user: The ad_object to use as template for MO.
        :return: uuid of the the user.
        """
        cpr_raw = ad_user.get(cpr_field)
        if cpr_raw is None:
            return None
        cpr = cpr_raw.replace("-", "")

        payload = payloads.create_user(cpr, ad_user, self.org_uuid, uuid=uuid)
        logger.info("Create user payload: {}".format(payload))
        r = self.helper._mo_post("e/create", payload)
        assert r.status_code == 201
        user_uuid = UUID(r.json())
        logger.info("Created employee {}".format(user_uuid))
        return user_uuid

    def _connect_user_to_ad(self, mo_uuid: str, username: str) -> None:
        """Write user AD username to the AD it system"""

        logger.info("Connect user to AD: {}".format(username))

        payload = payloads.connect_it_system_to_user(
            str(mo_uuid), username, str(self.AD_it_system_uuid), from_date=self.run_date
        )
        logger.debug("AD account payload: {}".format(payload))
        response = self.helper._mo_post("details/create", payload)
        assert response.status_code == 201
        logger.debug("Added AD account info to {}".format(username))

    def _create_engagement(
        self, ad_user: Dict, uuids: Dict[str, UUID], mo_uuid: Optional[UUID] = None
    ) -> None:
        """Create the engagement in MO"""
        # TODO: Check if we have start/end date of engagements in AD
        validity = {"from": self.run_date, "to": None}

        person_uuid = ad_user["ObjectGUID"]
        if mo_uuid:
            person_uuid = mo_uuid

        # TODO: Check if we can use job title from AD

        # Convert `uuid.UUID` values to strings before passing to `create_engagement`
        payload_kwargs = {k: str(v) for k, v in uuids.items()}
        payload = payloads.create_engagement(
            ad_user=ad_user,
            validity=validity,
            person_uuid=str(person_uuid),
            **payload_kwargs,
        )
        logger.info("Create engagement payload: {}".format(payload))
        response = self.helper._mo_post("details/create", payload)
        assert response.status_code == 201
        logger.info("Added engagement to {}".format(ad_user["SamAccountName"]))

    def cleanup_removed_users_from_mo(self) -> None:
        """Remove users in MO if they are no longer found as external users in AD."""
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        users = self._find_ou_users_in_ad()
        active_account_names = set(map(itemgetter("SamAccountName"), users.values()))

        mo_users = self.helper.read_organisation_people(self.root_ou_uuid)

        for mo_user_uuid, user in mo_users.items():
            AD_accounts = self.helper.get_e_itsystems(
                mo_user_uuid, it_system_uuid=self.AD_it_system_uuid
            )
            for account in AD_accounts:
                if account["user_key"] not in active_account_names:
                    # This users is in MO but not in AD:
                    logger.info(f"Terminating user with uuid={user['Person UUID']}")
                    payload = payloads.terminate_engagement(
                        user["Engagement UUID"], yesterday
                    )
                    logger.debug("Terminate payload: {}".format(payload))
                    response = self.helper._mo_post("details/terminate", payload)
                    logger.debug("Terminate response: {}".format(response.text))
                    response.raise_for_status()

                    # terminate username
                    payload["uuid"] = account["uuid"]
                    payload["type"] = "it"
                    logger.debug("Terminate it payload: {}".format(payload))
                    response = self.helper._mo_post("details/terminate", payload)
                    logger.debug("Terminate it response: {}".format(response.text))
                    response.raise_for_status()

    def create_or_update_users_in_mo(self) -> None:
        """
        Create users in MO that exist in AD but not in MO.
        Update name of users that has changed name in AD.
        """

        uuids = self._find_or_create_unit_and_classes()
        users = self._find_ou_users_in_ad()
        for AD in self.settings["integrations.ad"]:
            cpr_field = AD["cpr_field"]

            for user_uuid, ad_user in tqdm(
                users.items(), unit="Users", desc="Updating users"
            ):
                logger.info("Updating {}".format(ad_user["SamAccountName"]))
                cpr = ad_user[cpr_field]
                # Sometimes there is a temporary change of cpr in wich the
                # last character is replaced with an 'x'.
                # This user is ignored by the importer
                # until the cpr has been changed back.
                if cpr[-1].lower() == "x":
                    logger.info("Skipped due to 'x' in cpr.")
                    continue

                mo_user = self.helper.read_user(user_cpr=cpr)
                logger.info("Existing MO info: {}".format(mo_user))

                if mo_user:
                    mo_uuid = mo_user.get("uuid")
                else:
                    mo_uuid = self._create_user(ad_user, cpr_field)

                AD_username = self.helper.get_e_itsystems(
                    mo_uuid, self.AD_it_system_uuid
                )

                if not AD_username:
                    self._connect_user_to_ad(mo_uuid, ad_user["SamAccountName"])

                current_engagements = self.helper.read_user_engagement(user=mo_uuid)
                this_engagement = list(
                    filter(
                        lambda x: x.get("org_unit").get("uuid") == uuids["unit_uuid"],
                        current_engagements,
                    )
                )
                if not this_engagement:
                    self._create_engagement(ad_user, uuids, mo_uuid)


@click.command(help="AD->MO user import")
@optgroup.group("Action", cls=RequiredMutuallyExclusiveOptionGroup)
@optgroup.option("--create-or-update", is_flag=True)
@optgroup.option("--cleanup-removed-users", is_flag=True)
@optgroup.option("--full-sync", is_flag=True)
def import_ad_group(**args):
    """
    Command line interface for the AD to MO user import.
    """
    ad_import = ADMOImporter()

    if "crontab.SENTRY_DSN" in ad_import.settings:
        sentry_sdk.init(dsn=ad_import.settings["crontab.SENTRY_DSN"])

    if args.get("create_or_update"):
        ad_import.create_or_update_users_in_mo()

    if args.get("cleanup_removed_users"):
        ad_import.cleanup_removed_users_from_mo()

    if args.get("full_sync"):
        ad_import.create_or_update_users_in_mo()
        ad_import.cleanup_removed_users_from_mo()


if __name__ == "__main__":
    start_logging(LOG_FILE)
    import_ad_group()

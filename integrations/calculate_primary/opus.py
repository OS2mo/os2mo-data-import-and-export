import math

from integrations.calculate_primary.common import logger
from integrations.calculate_primary.common import MOPrimaryEngagementUpdater


class OPUSPrimaryEngagementUpdater(MOPrimaryEngagementUpdater):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Currently primary is set first by engagement type (order given in
        # settings) and secondly by job_id.
        # TODO: Check that configured eng_types exist
        self.eng_types_order = self.settings[
            "integrations.opus.eng_types_primary_order"
        ]

        def engagements_included_in_primary_calculation(user_uuid, no_past, engagement):
            if engagement["org_unit"]["uuid"] in self.settings.get(
                "integrations.ad.import_ou.mo_unit_uuid", ""
            ):
                # disregard engagements from externals
                logger.warning(
                    "disregarding external engagement: {}".format(engagement)
                )
                return False
            return True

        def remove_missing_user_key(user_uuid, no_past, engagement):
            return "user_key" in engagement

        self.calculate_filters = [
            engagements_included_in_primary_calculation,
            remove_missing_user_key,
        ]

    def _find_primary_types(self):
        """
        Read the engagement types from MO and match them up against the three
        known types in the OPUS->MO import.
        :param helper: An instance of mora-helpers.
        :return: A dict matching up the engagement types with LoRa class uuids.
        """
        # These constants are global in all OPUS municipalities (because they are
        # created by the OPUS->MO importer.
        PRIMARY = "primary"
        NON_PRIMARY = "non-primary"
        FIXED_PRIMARY = "explicitly-primary"

        logger.info("Read primary types")
        primary_dict = {"fixed_primary": None, "primary": None, "non_primary": None}

        primary_types = self.helper.read_classes_in_facet("primary_type")
        for primary_type in primary_types[0]:
            if primary_type["user_key"] == PRIMARY:
                primary_dict["primary"] = primary_type["uuid"]
            if primary_type["user_key"] == NON_PRIMARY:
                primary_dict["non_primary"] = primary_type["uuid"]
            if primary_type["user_key"] == FIXED_PRIMARY:
                primary_dict["fixed_primary"] = primary_type["uuid"]

        if None in primary_dict.values():
            raise Exception("Missing primary types: {}".format(primary_dict))
        primary_list = [primary_dict["fixed_primary"], primary_dict["primary"]]

        return primary_dict, primary_list

    def _find_primary(self, mo_engagements):
        # The primary engagement is the engagement with the lowest engagement type.
        # - The order of engagement types is given by self.eng_types_order.
        #
        # If two engagements have the same engagement_type, the tie is broken by
        # picking the one with the lowest user-key integer.
        def get_engagement_type_id(engagement):
            if engagement["engagement_type"]["uuid"] in self.eng_types_order:
                return self.eng_types_order.index(engagement["engagement_type"]["uuid"])
            return math.inf

        def get_engagement_order(engagement):
            try:
                eng_id = int(engagement["user_key"])
                return eng_id
            except Exception as exp:
                logger.warning(
                    "Skippning engangement with non-integer employment_id: {}".format(
                        engagement["user_key"]
                    )
                )
                logger.exception(exp)
                return math.inf

        primary_engagement = min(
            mo_engagements,
            # Sort first by engagement_type, then by user_key integer
            key=lambda eng: (get_engagement_type_id(eng), get_engagement_order(eng)),
        )
        return primary_engagement["uuid"]

from integrations.calculate_primary.common import (MOPrimaryEngagementUpdater,
                                                   logger)


class OPUSPrimaryEngagementUpdater(MOPrimaryEngagementUpdater):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Currently primary is set first by engagement type (order given in
        # settings) and secondly by job_id.
        self.eng_types_order = self.settings[
            "integrations.opus.eng_types_primary_order"
        ]

        def engagements_included_in_primary_calculation(user_uuid, no_past, engagement):
            if engagement["org_unit"]["uuid"] in self.settings.get(
                "integrations.ad.import_ou.mo_unit_uuid", ""
            ):
                # disregard engagements from externals
                logger.warning("disregarding external engagement: {}".format(eng))
                return False
            return True

        self.calculate_filters = [
            engagements_included_in_primary_calculation,
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
        # Ensure that all mo_engagements have user_keys.
        for eng in mo_engagements:
            if 'user_key' not in eng:
                return None

        def non_integer_userkey(eng):
            try:
                # non-integer user keys should not occur
                int(eng['user_key'])
            except ValueError:
                logger.warning(
                    "Skippning engangement with non-integer employment_id: {}".format(
                        eng["user_key"]
                    )
                )
                return False
            return True

        # Ensure that all mo_engagements have integer user_keys.
        mo_engagements = list(filter(non_integer_userkey, mo_engagements))
        if mo_engagements == []:
            return None
        # The primary engagement is the engagement with the lowest engagement type.
        # - The order of engagement types is given by self.eng_types_order.
        #
        # If two engagements have the same engagement_type, the tie is broken by
        # picking the one with the lowest user-key integer.
        def get_engagement_type_id(engagement):
            if eng["engagement_type"] in self.eng_types_order:
                return self.eng_types_order.index(eng["engagement_type"])
            return 9999

        primary_engagement = min(
            mo_engagements,
            # Sort first by engagement_type, then by user_key integer
            key=lambda eng: (get_engagement_type_id(eng), int(eng["user_key"]))
        )
        return primary_engagement['uuid']

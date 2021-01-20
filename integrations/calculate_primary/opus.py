from integrations.calculate_primary.common import (MOPrimaryEngagementUpdater,
                                                   logger)


class OPUSPrimaryEngagementUpdater(MOPrimaryEngagementUpdater):
    def __init__(self):
        super().__init__()
        # Currently primary is set first by engagement type (order given in
        # settings) and secondly by job_id.
        self.eng_types_order = self.settings[
            "integrations.opus.eng_types_primary_order"
        ]

        def engagements_included_in_primary_calculation(self, engagement):
            if eng["org_unit"]["uuid"] in self.settings.get(
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

    def _calculate_rate_and_ids(self, mo_engagement, no_past):
        min_type_pri = 9999
        min_id = 9999999
        for eng in mo_engagement:
            logger.debug("Calculate rate, engagement: {}".format(eng))

            try:
                employment_id = int(eng["user_key"])
            except ValueError:
                logger.warning(
                    "Skippning engangement with non-integer employment_id: {}".format(
                        eng["user_key"]
                    )
                )
                continue

            stat = "Current eng_type, min_id: {}, {}. This rate, eng_pos: {}, {}"
            logger.debug(
                stat.format(min_type_pri, min_id, employment_id, eng["fraction"])
            )

            if eng["engagement_type"] in self.eng_types_order:
                type_pri = self.eng_types_order.index(eng["engagement_type"])
            else:
                type_pri = 9999

            if type_pri == min_type_pri:
                if employment_id < min_id:
                    min_id = employment_id
            if type_pri < min_type_pri:
                min_id = employment_id
                min_type_pri = type_pri

        logger.debug("Min id: {}, Prioritied type: {}".format(min_id, min_type_pri))
        if (min_id is None) or (min_type_pri is None):
            raise Exception("Cannot calculate primary")
        return (min_id, min_type_pri)

    def _handle_non_integer_employment_id(self, validity, eng):
        logger.warning(
            "Skippning engangement with non-integer employment_id: {}".format(
                eng["user_key"]
            )
        )

    def _is_primary(self, employment_id, eng, min_id, impl_specific):
        min_type_pri = impl_specific

        if eng["engagement_type"] in self.eng_types_order:
            type_pri = self.eng_types_order.index(eng["engagement_type"])
        else:
            type_pri = 9999

        msg = "Current type pri and id: {}, {}"
        logger.debug(msg.format(type_pri, employment_id))

        return type_pri == min_type_pri and employment_id == min_id

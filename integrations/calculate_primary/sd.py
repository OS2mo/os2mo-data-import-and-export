import datetime

from integrations.calculate_primary.common import logger
from integrations.calculate_primary.common import MOPrimaryEngagementUpdater


# This function has been copied directly from the SD-integration repo as
# it was missing when the SD-integration was moved to its own repo. Ideally,
# it should have been moved to a library and used both in DIPEX and in the
# SD-integration, but it is probably not worth the effort, since it is not
# unlikely that it will be removed from the SD-integration anyway as this
# integration should not have the responsibility of calculating primary
# engagements
def get_primary_types(helper):
    """
    Read the engagement types from MO and match them up against the four
    known types in the SD->MO import.

    Args:
        helper: An instance of mora-helpers.

    Returns:
        A dict matching up the engagement types with LoRa class uuids (
        i.e. UUIDs of facets).

    Example:
        An example return value:
        ```python
        {
            "primary": "697c8838-ba0f-4e74-90f8-4e7c31d4e7e7",
            "non_primary": "b9543f90-9511-494b-bbf5-f15678502c2d",
            "fixed_primary": "c95a1999-9f95-4458-a218-e9c96e7ad3db",
        }
        ```
    """

    # These constants are global in all SD municipalities (because they are created
    # by the SD->MO importer.
    PRIMARY = "Ansat"
    NON_PRIMARY = "non-primary"
    FIXED_PRIMARY = "explicitly-primary"

    logger.info("Read primary types")
    primary = None
    non_primary = None
    fixed_primary = None

    primary_types = helper.read_classes_in_facet("primary_type")
    for primary_type in primary_types[0]:
        if primary_type["user_key"] == PRIMARY:
            primary = primary_type["uuid"]
        if primary_type["user_key"] == NON_PRIMARY:
            non_primary = primary_type["uuid"]
        if primary_type["user_key"] == FIXED_PRIMARY:
            fixed_primary = primary_type["uuid"]

    type_uuids = {
        "primary": primary,
        "non_primary": non_primary,
        "fixed_primary": fixed_primary,
    }
    if None in type_uuids.values():
        raise Exception("Missing primary types: {}".format(type_uuids))
    return type_uuids


class SDPrimaryEngagementUpdater(MOPrimaryEngagementUpdater):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def remove_past(user_uuid, no_past, eng):
            if no_past and eng["validity"]["to"]:
                to = datetime.datetime.strptime(eng["validity"]["to"], "%Y-%m-%d")
                if to < datetime.datetime.now():
                    return False
            return True

        def remove_missing_user_key(user_uuid, no_past, eng):
            return "user_key" in eng

        self.calculate_filters = [
            remove_past,
            remove_missing_user_key,
        ]

    def _find_primary_types(self):
        # Keys are; fixed_primary, primary and non-primary
        primary_types = get_primary_types(self.helper)
        primary = [
            primary_types["fixed_primary"],
            primary_types["primary"],
        ]
        return primary_types, primary

    def _find_primary(self, mo_engagements):
        def non_integer_userkey(mo_engagement):
            try:
                # non-integer user keys should universally be status0, and as such
                # they should already have been filtered out, thus if they have not
                # been filtered out, they must have the wrong primary_type.
                int(mo_engagement["user_key"])
            except ValueError:
                # Filter it out, as it should have been
                return False
            return True

        # Ensure that all mo_engagements have integer user_keys.
        mo_engagements = list(filter(non_integer_userkey, mo_engagements))

        if not mo_engagements:
            return None

        # The primary engagement is the engagement with the highest occupation rate.
        # - The occupation rate is found as 'fraction' on the engagement.
        #
        # If two engagements have the same occupation rate, the tie is broken by
        # picking the one with the lowest user-key integer.
        primary_engagement = max(
            mo_engagements,
            # Sort first by fraction, then reversely by user_key integer
            key=lambda eng: (eng.get("fraction") or 0, -int(eng["user_key"])),
        )
        return primary_engagement["uuid"]

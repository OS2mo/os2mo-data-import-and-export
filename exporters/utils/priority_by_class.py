from more_itertools import first


def choose_public_address_helper(
    candidates, prioritized_classes, scope_getter, address_type_getter, uuid_getter
):
    """Pick the most desirable valid candidate address.

    An address candidate is considered valid if its visibility is PUBLIC or UNSET.

    An address candidates desirability is inversely propertional to its address
    types position inside prioritized_classes. I.e. address candidates whose address
    types occur earlier in prioritized_classes are more desirable than address
    candidates whose address types occur later (or yet worse, not at all).
    Additionally lower UUIDs are considered more desirable than larger ones, this is
    to ensure a consistent order regardless of the order of the candidates.

    Args:
        candidates: List of Address entries.
        prioritized_classes: List of Address Type UUIDs.
        scope_getter: Function from address entry to visibility scope.
        address_type_getter: Function from address entry to address type uuid.
        uuid_getter: Function from address entry to address entry uuid.

    Returns:
        Address entry: The address entry that is the most desirable.
    """

    def filter_by_visibility(candidate):
        """Predicate for filtering on visibility.

        Args:
            candidate: Address entry.

        Returns:
            bool: True for candidates with PUBLIC or UNSET visibility.
                  False otherwise.
        """
        visibility_scope = scope_getter(candidate)
        return visibility_scope is None or visibility_scope == "PUBLIC"

    def determine_candidate_desirability(candidate):
        """Predicate for determining desirability of an address candidate.

        The lower the value returned, the more desirable the candidate is.

        Args:
            candidate: Address entry.

        Returns:
            int: Index of the candidates address_type inside prioritized_classes.
                 Length of prioritized_classes if no match is found.
        """
        address_type_uuid = address_type_getter(candidate)
        try:
            priority = prioritized_classes.index(address_type_uuid)
        except ValueError:
            priority = len(prioritized_classes)
        return priority, uuid_getter(candidate)

    # Filter candidates to only keep valid ones
    candidates = filter(filter_by_visibility, candidates)

    # If no prioritized_classes are provided, all the entries are equally desirable.
    # Thus we can just return the entry with the lowest uuid.
    if not prioritized_classes:
        return min(candidates, key=uuid_getter, default=None)

    # If prioritized_classes are provided, we want to return the most desirable one.
    # The lowest index is the most desirable.
    return min(candidates, key=determine_candidate_desirability, default=None)


def mora_choose_public_address(candidates, prioritized_classes):
    """See choose_public_address_helper.

    Candidates are a list of MO address entries.
    """

    def scope_getter(candidate):
        if "visibility" not in candidate:
            return None
        if candidate["visibility"] is None:
            return None
        return candidate["visibility"]["scope"]

    def address_type_getter(candidate):
        return candidate["address_type"]["uuid"]

    def uuid_getter(candidate):
        return candidate["uuid"]

    return choose_public_address_helper(
        candidates,
        prioritized_classes,
        scope_getter,
        address_type_getter,
        uuid_getter,
    )


def lc_choose_public_address(candidates, prioritized_classes, lc):
    """See choose_public_address_helper.

    Candidates are a list of LoraCache address entries.
    """

    def scope_getter(candidate):
        if "visibility" not in candidate:
            return None
        if candidate["visibility"] is None:
            return None
        return lc.classes[candidate["visibility"]]["scope"]

    def address_type_getter(candidate):
        return candidate["adresse_type"]

    def uuid_getter(candidate):
        return candidate["uuid"]

    return choose_public_address_helper(
        candidates,
        prioritized_classes,
        scope_getter,
        address_type_getter,
        uuid_getter,
    )


def lcdb_choose_public_address(candidates, prioritized_classes):
    """See choose_public_address_helper.

    Candidates are a list of LoraCache sqlalchemy address entries.
    """

    def scope_getter(candidate):
        scope = candidate.synlighed_scope
        return scope or None

    def address_type_getter(candidate):
        return candidate.adressetype_uuid

    def uuid_getter(candidate):
        return candidate.uuid

    return choose_public_address_helper(
        candidates,
        prioritized_classes,
        scope_getter,
        address_type_getter,
        uuid_getter,
    )


def choose_public_address(candidates, prioritized_classes):
    """See mora_choose_public_address."""
    return mora_choose_public_address(candidates, prioritized_classes)

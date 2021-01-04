from more_itertools import first


def choose_public_address_helper(candidates, prioritized_classes, scope_getter, address_type_getter):
    """Pick the most desirable valid candidate address.

    An address candidate is considered valid if its visibility is PUBLIC or UNSET.

    An address candidates desirability is inversely propertional to its address
    types position inside prioritized_classes. I.e. address candidates whose address
    types occur earlier in prioritized_classes are more desirable than address
    candidates whose address types occur later (or yet worse, not at all).

    Args:
        candidates: List of Address entries.
        prioritized_classes: List of Address Type UUIDs.
        scope_getter: Function from address entry to visibility scope.
        address_type_getter: Functon from address entry to address type uuid.

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
        return (
            candidate["visibility"] is None or
            scope_getter(candidate) == "PUBLIC"
        )

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
        return priority

    # Filter candidates to only keep valid ones
    candidates = filter(filter_by_visibility, candidates)

    # If no prioritized_classes are provided, all the entries are equally desirable.
    # Thus we can just return the first entry.
    if not prioritized_classes:
        return first(candidates, None)

    # If prioritized_classes are provided, we want to return the most desirable one.
    # The lowest index is the most desirable.
    return min(candidates, key=determine_candidate_desirability, default=None)


def mora_choose_public_address(candidates, prioritized_classes):
    """See choose_public_address_helper."""
    def scope_getter(candidate):
        return candidate["visibility"]["scope"]

    def address_type_getter(candidate):
        return candidate["address_type"]["uuid"] 

    return choose_public_address_helper(candidates, prioritized_classes, scope_getter, address_type_getter)


def lc_choose_public_address(candidates, prioritized_classes, lc):
    """See choose_public_address_helper."""
    def scope_getter(candidate):
        return lc.classes[candidate["visibility"]]["scope"]

    def address_type_getter(candidate):
        return candidate["adresse_type"]

    return choose_public_address_helper(candidates, prioritized_classes, scope_getter, address_type_getter)


def choose_public_address(candidates, prioritized_classes):
    """See mora_choose_public_address."""
    return mora_choose_public_address(candidates, prioritized_classes)

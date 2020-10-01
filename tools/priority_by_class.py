def choose_public_address(candidates, prioritized_classes):
    # choose using prioritized list if available, 
    # else PUBLIC and last those without visibility
    candidates = sorted(candidates, key=lambda x: (
                        x.get("visibility") is None, x.get("visibility")))
    chosen = None
    for cls in prioritized_classes:
        if chosen:
            break
        for candidate in candidates:
            if (
                candidate["address_type"]["uuid"] == cls
                and candidate.get("visibility",
                                  {"scope": "PUBLIC"})["scope"] == "PUBLIC"
            ):
                chosen = candidate
                break

    if not prioritized_classes:
        for candidate in candidates:
            if candidate.get("visibility",
                             {"scope": "PUBLIC"})["scope"] == "PUBLIC":
                chosen = candidate
                break
    return chosen

def lc_choose_public_address(candidates, prioritized_classes, lc):
    # choose using prioritized list if available
    # else PUBLIC and last those without visibility
    candidates = sorted(candidates, key=lambda x: (
                        x.get("visibility") is None, x.get("visibility")))
    chosen = None
    for cls in prioritized_classes:
        if chosen:
            break
        for candidate in candidates:
            if (
                candidate["adresse_type"] == cls
                and (
                    candidate["visibility"] is None
                    or lc.classes[candidate["visibility"]]["scope"] == "PUBLIC"
                )
            ):
                chosen = candidate
                break

    if not prioritized_classes:
        for candidate in candidates:
            if (
                candidate["visibility"] is None
                or lc.classes[candidate["visibility"]]["scope"] == "PUBLIC"
            ):
                chosen = candidate
                break

    return chosen


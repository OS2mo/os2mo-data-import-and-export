# SPDX-FileCopyrightText: 2021 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
from typing import Dict
from typing import List
from typing import Sequence
from typing import Tuple

import click
from more_itertools import one
from more_itertools import pairwise
from psycopg2 import connect
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor
from tqdm import tqdm


def get_connection(
    user: str, dbname: str, host: str, password: str, port: int
) -> connection:
    """
    Establish connection to mox-db.
    """
    return connect(
        user=user,
        dbname=dbname,
        host=host,
        password=password,
        port=port,
    )


def get_unique_bruger_registrering(connector: connection) -> List[int]:
    """
    Fetch all bruger_registration IDs.
    """
    cursor = connector.cursor()
    cursor.execute(
        """
        SELECT id
        FROM bruger_registrering
        """
    )
    rows = cursor.fetchall()
    ids = list(map(one, rows))
    return ids


def get_table_for_registrering(
    connector: connection,
    registrering_id: int,
    table: str,
    equivalence_keys: Sequence[str],
):
    """
    Get table data for a single registration.
    """
    cursor = connector.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        f"""
        SELECT id, {", ".join(equivalence_keys)}, (virkning).TimePeriod as virkning
        FROM {table}
        WHERE bruger_registrering_id = %(registrering_id)s
        ORDER BY virkning
        """,
        {"registrering_id": registrering_id},
    )
    rows = cursor.fetchall()
    return rows


def collapse_table(
    rows: List[dict], equivalence_keys: Sequence[str]
) -> Tuple[List[int], Dict[int, str]]:
    """
    Consolidate table rows.
    """
    delete_rows: List[int] = []
    update_rows: Dict[int, str] = {}

    for l, r in pairwise(rows):
        equal = all(l[key] == r[key] for key in equivalence_keys)
        if equal:
            # We are merging left into right, left should be deleted and not updated.
            delete_rows.append(l["id"])
            update_rows.pop(l["id"], None)
            # Right now gets the start-time from left and should be updated
            r["virkning"]._lower = l["virkning"]._lower
            update_rows[r["id"]] = r["virkning"]
    return delete_rows, update_rows


def effectuate_table(
    connector: connection,
    delete_rows: List[int],
    update_rows: Dict[int, str],
    table: str,
):
    """
    Send updated data back to the database.
    """
    cursor = connector.cursor(cursor_factory=RealDictCursor)
    cursor.execute("BEGIN")

    if delete_rows:
        if len(delete_rows) == 1:
            delete_statement = f"WHERE id = {delete_rows[0]}"
        else:
            delete_statement = f"WHERE id IN {tuple(delete_rows)}"
        cursor.execute(
            f"""
            DELETE FROM {table}
            {delete_statement}
            """,
        )
    for id, virkning in update_rows.items():
        if virkning._upper.year == 9999:
            virkning._upper = "infinity"  # lol
        cursor.execute(
            f"""
            UPDATE {table}
            SET virkning.TimePeriod=%(virkning)s
            WHERE id = %(id)s;
            """,
            {"id": id, "virkning": virkning},
        )
    cursor.execute("COMMIT")


table_and_equivalence_keys = {
    "bruger_attr_egenskaber": (
        "brugervendtnoegle",
        "brugernavn",
        "brugertype",
        "integrationsdata",
    ),
    "bruger_attr_udvidelser": (
        "fornavn",
        "efternavn",
        "kaldenavn_fornavn",
        "kaldenavn_efternavn",
        "seniority",
    ),
    "bruger_relation": (
        "rel_maal_uuid",
        "rel_maal_urn",
        "rel_type",
        "objekt_type",
    ),
    "bruger_tils_gyldighed": ("gyldighed",),
}


@click.command()
@click.option("--db-user", required=True)
@click.option("--db-name", required=True)
@click.option("--db-host", required=True)
@click.option("--db-password", required=True)
@click.option("--db-port", type=int, required=True)
def run(db_user: str, db_name: str, db_host: str, db_password: str, db_port: int):
    connector = get_connection(
        user=db_user,
        dbname=db_name,
        host=db_host,
        password=db_password,
        port=db_port,
    )
    for table, equivalence_keys in table_and_equivalence_keys.items():
        for bruger_registrering_id in tqdm(get_unique_bruger_registrering(connector)):
            bruger_attr_rows = get_table_for_registrering(
                connector=connector,
                registrering_id=bruger_registrering_id,
                table=table,
                equivalence_keys=equivalence_keys,
            )
            delete_rows, update_rows = collapse_table(
                rows=bruger_attr_rows,
                equivalence_keys=equivalence_keys,
            )
            effectuate_table(
                connector=connector,
                delete_rows=delete_rows,
                update_rows=update_rows,
                table=table,
            )


if __name__ == "__main__":
    run()

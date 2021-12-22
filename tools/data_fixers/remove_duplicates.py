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
    print("Connecting to the database")
    return connect(
        user=user,
        dbname=dbname,
        host=host,
        password=password,
        port=port,
    )


def create_indexes(connector: connection) -> None:
    """
    These indexes are required to run the script in a time resembling polynomial.
    """
    print("Creating indexes")
    cursor = connector.cursor()
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS bruger_attr_egenskaber_bruger_registrering_id_index
        ON bruger_attr_egenskaber (bruger_registrering_id);
        CREATE INDEX IF NOT EXISTS bruger_attr_udvidelser_bruger_registrering_id_index
        ON bruger_attr_udvidelser (bruger_registrering_id);
        CREATE INDEX IF NOT EXISTS bruger_tils_gyldighed_bruger_registrering_id_index
        ON bruger_tils_gyldighed (bruger_registrering_id);
        CREATE INDEX IF NOT EXISTS bruger_relation_bruger_registrering_id_index
        ON bruger_relation (bruger_registrering_id);
        """
    )


def delete_indexes(connector: connection) -> None:
    """
    Dropping these unused indexes dramatically speeds up execution.
    """
    print("Dropping useless indexes")
    cursor = connector.cursor()
    cursor.execute(
        """
        DROP INDEX IF EXISTS bruger_attr_egenskaber_idx_virkning_notetekst;
        DROP INDEX IF EXISTS bruger_attr_egenskaber_idx_brugervendtnoegle;
        DROP INDEX IF EXISTS bruger_attr_egenskaber_idx_brugernavn;
        DROP INDEX IF EXISTS bruger_attr_egenskaber_idx_brugertype;
        DROP INDEX IF EXISTS bruger_attr_egenskaber_idx_integrationsdata;
        DROP INDEX IF EXISTS bruger_attr_egenskaber_idx_virkning_aktoerref;
        DROP INDEX IF EXISTS bruger_attr_egenskaber_pat_virkning_notetekst;
        DROP INDEX IF EXISTS bruger_attr_egenskaber_idx_virkning_aktoertypekode;
        DROP INDEX IF EXISTS bruger_attr_udvidelser_idx_kaldenavn_fornavn;
        DROP INDEX IF EXISTS bruger_attr_udvidelser_idx_fornavn;
        DROP INDEX IF EXISTS bruger_attr_udvidelser_idx_efternavn;
        DROP INDEX IF EXISTS bruger_attr_udvidelser_idx_kaldenavn_efternavn;
        DROP INDEX IF EXISTS bruger_attr_udvidelser_idx_seniority;
        DROP INDEX IF EXISTS bruger_attr_udvidelser_idx_virkning_aktoerref;
        DROP INDEX IF EXISTS bruger_attr_udvidelser_idx_virkning_aktoertypekode;
        DROP INDEX IF EXISTS bruger_attr_udvidelser_idx_virkning_notetekst;
        DROP INDEX IF EXISTS bruger_attr_udvidelser_pat_virkning_notetekst;
        DROP INDEX IF EXISTS bruger_registrering_idx_livscykluskode;
        DROP INDEX IF EXISTS bruger_registrering_idx_brugerref;
        DROP INDEX IF EXISTS bruger_registrering_idx_note;
        DROP INDEX IF EXISTS bruger_registrering_pat_note;
        DROP INDEX IF EXISTS bruger_relation_idx_rel_maal_uuid;
        DROP INDEX IF EXISTS bruger_relation_idx_rel_maal_urn_isolated;
        DROP INDEX IF EXISTS bruger_relation_idx_rel_maal_obj_uuid;
        DROP INDEX IF EXISTS bruger_relation_idx_rel_maal_obj_urn;
        DROP INDEX IF EXISTS bruger_relation_idx_rel_maal_uuid_isolated;
        DROP INDEX IF EXISTS bruger_relation_idx_virkning_aktoertypekode;
        DROP INDEX IF EXISTS bruger_relation_idx_virkning_notetekst;
        DROP INDEX IF EXISTS bruger_relation_pat_virkning_notetekst;
        DROP INDEX IF EXISTS bruger_relation_idx_virkning_aktoerref;
        DROP INDEX IF EXISTS bruger_tils_gyldighed_idx_gyldighed;
        DROP INDEX IF EXISTS bruger_tils_gyldighed_idx_virkning_aktoerref;
        """
    )


def get_unique_bruger_registrering(connector: connection) -> List[int]:
    """
    Fetch all bruger_registration IDs.
    """
    print("Getting bruger_registration IDs")
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
    delete_indexes(connector)
    create_indexes(connector)
    for table, equivalence_keys in table_and_equivalence_keys.items():
        print("Removing duplicates for", table)
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

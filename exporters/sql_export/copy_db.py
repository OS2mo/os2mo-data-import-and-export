from typing import Optional

import click
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import get_class_by_table
from tqdm import tqdm

from sql_table_defs import Base
from sql_url import DatabaseFunction, generate_connection_url


def recreate_db(session) -> None:
    """Drop and (re)create all tables."""
    connection = session.connection()

    Base.metadata.drop_all(connection)
    Base.metadata.create_all(connection)


def transfer_tables(from_session, to_session) -> None:
    """Transfer all data from from_session to to_session."""
    # Pull list of defined models in our Base
    models = [get_class_by_table(Base, table) for table in Base.metadata.tables.values()]

    for model in tqdm(models, desc="Migrating models"):
        modelname = model.__name__
        # Convert from_session model into to_session model
        from_entries = from_session.query(model).all()
        count = len(from_entries)
        to_entries = map(to_session.merge, from_entries)
        for to_entry in tqdm(to_entries, desc=f"Transfering {modelname}", total=count):
            # Convert from_session model into to_session model
            to_session.add(to_entry)


def create_session(connection_url):
    """Create a database session from a connection url."""
    engine = create_engine(connection_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


@click.command()
@click.option("--from-connection-url")
@click.option("--to-connection-url")
def main(
    from_connection_url: Optional[str],
    to_connection_url: Optional[str],
) -> None:
    if from_connection_url is None:
        from_connection_url = generate_connection_url(
            DatabaseFunction.ACTUAL_STATE_WRITER
        )
    if to_connection_url is None:
        to_connection_url = generate_connection_url(DatabaseFunction.ACTUAL_STATE)

    from_session = create_session(from_connection_url)
    to_session = create_session(to_connection_url)

    recreate_db(to_session)
    transfer_tables(from_session, to_session)

    to_session.commit()


if __name__ == "__main__":
    main()

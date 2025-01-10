from io import BytesIO
from socket import timeout  # Used to handle exception
from typing import Any

import click
import more_itertools

from ftplib import FTP_TLS
from gql import gql

from raclients.graph.client import GraphQLClient
from ra_utils.load_settings import load_settings


def start_ftpes_connection(timeout=30):
    ftps = FTP_TLS(timeout=timeout)
    ftps.connect(SETTINGS["exporters.plan2learn.host"], 21)
    ftps.auth()
    ftps.prot_p()
    ftps.login(
        SETTINGS["exporters.plan2learn.user"], SETTINGS["exporters.plan2learn.password"]
    )
    return ftps


def dir_list():
    ftps = start_ftpes_connection()
    dir_list = []
    ftps.dir(dir_list.append)
    for file_info in dir_list:
        print(file_info)
    ftps.quit()


def read_file(settings: dict[str, Any], filename: str) -> str:
    """Return the content of *filename* from OS2mo."""
    client = GraphQLClient(
        url=f"{settings['mora.base']}/graphql/v22",
        client_id=settings["crontab.CLIENT_ID"],
        client_secret=settings["crontab.CLIENT_SECRET"],
        auth_server=settings["crontab.AUTH_SERVER"],
        auth_realm="mo",
        sync=True,
    )

    query = gql(
        """
    query ReadFile($file_name: [String!]) {
      files(filter: {file_store: EXPORTS, file_names: $file_name}) {
        objects {
          text_contents
        }
      }
    }
    """
    )

    result = client.execute(query, {"file_name": filename})
    return more_itertools.one(result["files"]["objects"])["text_contents"]


SETTINGS = load_settings()


@click.command()
def main():
    print("Directory listing before upload:")
    dir_list()


    filenames = [
        "bruger.csv",
        "leder.csv",
        "engagement.csv",
        "organisation.csv",
        "stillingskode.csv",
    ]

    for to_file in filenames:
        # The server insists on returning a timeout after every upload, so we
        # re-start the connection for each file.
        ftps = start_ftpes_connection()
        from_file = f"plan2learn_{to_file}"

        csv_file = BytesIO(read_file(SETTINGS, from_file).encode())
        print("Uploading: {}".format(to_file))
        try:
            ftps.storbinary("STOR {}".format(to_file), csv_file)
        except ConnectionResetError:
            print("Connection Error")
        except timeout:
            print("Timeout Error")
        else:
            print("Done")
        finally:
            ftps.quit()

    print("Directory listing after upload:")
    dir_list()


if __name__ == "__main__":
    main()

from ftplib import FTP_TLS
from io import BytesIO
from socket import timeout  # Used to handle exception

import click
import more_itertools
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql

from exporters.plan2learn.plan2learn import Plan2LearnFTPES
from exporters.plan2learn.plan2learn import Settings
from exporters.plan2learn.plan2learn import get_unified_settings


def start_ftpes_connection(settings: Plan2LearnFTPES, timeout=30):
    ftps = FTP_TLS(timeout=timeout)
    ftps.connect(settings.hostname, settings.port)
    ftps.auth()
    ftps.prot_p()
    ftps.login(settings.username, settings.password.get_secret_value())
    return ftps


def print_dir_list(settings: Plan2LearnFTPES):
    ftps = start_ftpes_connection(settings=settings)
    dir_list: list[str] = []
    ftps.dir(dir_list.append)
    for file_info in dir_list:
        print(file_info)
    ftps.quit()


def read_file(settings: Settings, filename: str) -> str:
    """Return the content of *filename* from OS2mo."""
    client = GraphQLClient(
        url=f"{settings.mora_base}/graphql/v22",
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        auth_server=settings.auth_server,
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


@click.command()
@click.option("--kubernetes", is_flag=True, envvar="KUBERNETES")
def main(kubernetes: bool):
    settings = get_unified_settings(kubernetes_environment=kubernetes)
    if not settings.plan2_learn_ftpes:
        click.echo("No ftp-connection setup - aborting")
        return
    print("Directory listing before upload:")
    print_dir_list(settings.plan2_learn_ftpes)

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
        ftps = start_ftpes_connection(settings.plan2_learn_ftpes)
        from_file = f"plan2learn_{to_file}"

        csv_file = BytesIO(read_file(settings, from_file).encode())
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
    print_dir_list(settings.plan2_learn_ftpes)


if __name__ == "__main__":
    main()

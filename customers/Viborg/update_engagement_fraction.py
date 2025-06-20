import csv
from datetime import datetime
from zoneinfo import ZoneInfo

import click
from fastapi.encoders import jsonable_encoder
from fastramqpi.ra_utils.job_settings import JobSettings
from fastramqpi.raclients.graph.client import GraphQLClient
from gql import gql
from more_itertools import unique_everseen

tz = ZoneInfo("Europe/Copenhagen")

read_query = """
query ReadEngagementDetails($uuids: [UUID!], $from_date: DateTime!) {
  engagements(filter: { uuids: $uuids, from_date: $from_date, to_date: null }) {
    objects {
      validities {
        employee_uuid
        engagement_type_uuid
        extension_1
        extension_10
        extension_2
        extension_3
        extension_4
        extension_5
        extension_6
        extension_7
        extension_8
        extension_9
        fraction
        job_function_uuid
        org_unit_uuid
        primary_uuid
        user_key
        uuid
        validity {
          from
          to
        }
      }
      uuid
    }
  }
}
"""

update_query = """mutation UpdateEngagement(
  $engagement_type_uuid: UUID
  $extension_1: String
  $extension_10: String
  $extension_2: String
  $extension_3: String
  $extension_4: String
  $extension_5: String
  $extension_6: String
  $extension_7: String
  $extension_8: String
  $extension_9: String
  $fraction: Int
  $from: DateTime!
  $job_function_uuid: UUID
  $org_unit_uuid: UUID
  $employee_uuid: UUID
  $primary_uuid: UUID
  $user_key: String
  $uuid: UUID!
  $to: DateTime
) {
  engagement_update(
    input: {
      engagement_type: $engagement_type_uuid
      extension_1: $extension_1
      extension_10: $extension_10
      extension_2: $extension_2
      extension_3: $extension_3
      extension_4: $extension_4
      extension_5: $extension_5
      extension_6: $extension_6
      extension_7: $extension_7
      extension_8: $extension_8
      extension_9: $extension_9
      fraction: $fraction
      job_function: $job_function_uuid
      org_unit: $org_unit_uuid
      person: $employee_uuid
      primary: $primary_uuid
      user_key: $user_key
      uuid: $uuid
      validity: { from: $from, to: $to }
    }
  ) {
    uuid
  }
}
"""


def setup_gql_client(
    settings: JobSettings,
) -> GraphQLClient:
    return GraphQLClient(
        url=f"{settings.mora_base}/graphql/v25",
        client_id=settings.client_id,
        client_secret=settings.client_secret,  # type: ignore
        auth_realm=settings.auth_realm,
        auth_server=settings.auth_server,  # type: ignore
        sync=True,
        httpx_client_kwargs={"timeout": None},
    )


def read_file(filename) -> list[list[str]]:
    with open(filename, mode="r") as f:
        csv_file = list(csv.reader(f, delimiter="\t"))
    return csv_file


@click.command()
@click.argument("filename", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True)
def main(filename: str, dry_run: bool):
    lines = read_file(filename)

    settings = JobSettings()
    graphql_client = setup_gql_client(settings=settings)
    engagement_uuids = [line[0] for line in lines]

    engagement_data = graphql_client.execute(
        gql(read_query),
        variable_values=jsonable_encoder(
            {"uuids": engagement_uuids, "from_date": datetime.now(tz=tz)}
        ),
    )

    engagements_map = {
        eng["uuid"]: eng["validities"]
        for eng in engagement_data["engagements"]["objects"]
    }
    # Keep a set of all updated uuids to log how many engagements was updated
    updated_uuids = set()
    for uuid, input_fraction in unique_everseen(lines):
        # Convert inputs like "0,80000" to a float and
        # scale input fraction (0-1) to MOs values (0-1_000_000)
        fraction = int(float(input_fraction.replace(",", ".")) * 1_000_000)
        try:
            engagement_validities = engagements_map[uuid]
        except KeyError:
            click.echo(f"Warning: engagement with {uuid=} not found in MO")
            continue
        for validity_values in engagement_validities:
            if validity_values["fraction"] == fraction:
                # Nothing to update
                continue
            payload = validity_values.copy()
            payload["fraction"] = fraction

            payload["from"] = payload["validity"]["from"]
            payload["to"] = payload["validity"]["to"]

            if not dry_run:
                graphql_client.execute(gql(update_query), variable_values=payload)

            updated_uuids.add(uuid)

    click.echo(f"Done. Updated fractions for {len(updated_uuids)} engagements")


if __name__ == "__main__":
    main()

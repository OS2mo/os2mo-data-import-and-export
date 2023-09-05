import logging
from datetime import datetime
from datetime import timedelta
from pprint import pprint

from more_itertools import first
from raclients.graph.client import GraphQLClient

from exporters.sql_exporter.settings import SqlExporterSettings

logger = logging.getLogger(__name__)
class GraphQLJob:
    uuid: str
    query: str
    variables: dict


def get_uuid(obj: dict) -> str:
    return obj.get("uuid")


def get_first_uuid_from_list(uuids: list[dict[str, str]] | None) -> str | None:
    if uuids is None or len(uuids) == 0:
        return None

    list_of_uuids = [get_uuid(elem) for elem in uuids]
    if list_of_uuids is None or len(list_of_uuids) == 0:
        return None
    return list_of_uuids[0]


def get_uuid_from_nested_dict(obj: dict, key: str) -> str | None:
    tmp = obj.get(key)
    if tmp is None:
        return None
    return tmp.get("uuid")


def get_person_uuid(obj: dict) -> str | None:
    return get_first_uuid_from_list(obj.get("person"))


def get_org_unit_uuid(obj: dict) -> str | None:
    return get_first_uuid_from_list(obj.get("org_unit"))


def get_name(obj: dict) -> str | None:
    return obj.get("name")


def get_user_key(obj: dict) -> str:
    return obj.get("user_key")


def get_validity_start(obj: dict) -> str | None:
    validity = obj.get("validity")
    if validity is None or len(validity) == 0:
        return None
    return validity.get("from")
    # validity_str = validity.get("from")
    # if validity_str is not None and len(validity) > 0:
    #     datetime.fromisoformat(validity_str)
    # return None


def get_validity_end(obj: dict) -> str | None:
    validity = obj.get("validity")
    if validity is None or len(validity) == 0:
        return None
    return validity.get("to")
    # validity_str = validity.get("to")
    # if validity_str is not None and len(validity) > 0:
    #     datetime.fromisoformat(validity_str)
    # return None


def get_list_of_uuids(obj: dict, key: str) -> list[str] | None:
    obj_list = obj.get(key)
    if obj_list is None:
        return None
    return [get_uuid(item) for item in obj_list]


def append_validity_base(
        body: dict, validity_start: str | None, validity_end: str | None
) -> dict:
    body.update({"from_date": validity_start})
    body.update({"to_date": validity_end})
    return body


def get_historic_variables(historic: bool, skip_past: bool) -> dict:
    if not historic:
        return {}

    now = datetime.now()
    params: dict = {
        'from_date': None,
        'to_date': None
    }

    if skip_past:
        params.update({'from_date': now})
    return params


def get_query_header_and_footer(query_type: str, historic: bool = False) -> (str, str):
    query_footer = " } } } }"

    if not historic or query_type in ["facets", "classes", "itsystems"]:
        query_header = (
            "query ($uuids: [UUID!]) { item: " + query_type + " (uuids: $uuids){ "
            "list_of_objects: objects { uuid single_obj: current { "
        )
    else:
        query_header = (
            "query ($to_date: DateTime,"
            " $from_date: DateTime,"
            " $uuids: [UUID!]) { item: " + query_type + " (from_date: $from_date, "
            " to_date: $to_date,"
            " uuids: $uuids)"
            " { list_of_objects: objects { uuid single_obj: objects {"
        )

    return query_header, query_footer


def build_query(
    uuid: str, query_body: str, query_type, historic: bool, skip_past: bool
) -> GraphQLJob:
    job = GraphQLJob()

    header, footer = get_query_header_and_footer(query_type, historic)
    job.query = (header + query_body + footer)

    variables = get_historic_variables(historic, skip_past)
    variables["uuids"] = uuid
    job.variables = variables.copy()

    return job


def _setup_gql_client(settings: SqlExporterSettings) -> GraphQLClient:
    return GraphQLClient(
        url=f"{settings.mora_base}/graphql/v13",
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        auth_realm=settings.auth_realm,
        auth_server=settings.auth_server,
        httpx_client_kwargs={"timeout": 300},
        execute_timeout=300,
    )

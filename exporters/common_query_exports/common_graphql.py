from uuid import UUID
from typing import Any

from gql import gql
from gql.client import SyncClientSession


def get_managers_for_export(
        gql_session: SyncClientSession, org_uuid: list[UUID]
) -> list[dict[str, Any]]:
    """
    Makes a GraphQL call, to retrieve an Organisation Units' manager(s)
    and all relevant details.

    :args:
    GraphQL session
    Organisation Unit uuid.

    :returns:
    A list with a payload of manager object consisting of: responsibilities,
    persons name and addresses.

    :example:
    "[{'objects': [{'employee': [{'addresses': [{'address_type': {'scope': 'EMAIL'},
                                            'name': 'tracya@kolding.dk'},
                                           {'address_type': {'scope': 'DAR'},
                                            'name': 'Finmarken 94, 6000 '
                                                    'Kolding'},
                                           {'address_type': {'scope': 'PHONE'},
                                            'name': '67338448'}],
                             'name': 'Tracy Andersen'}],
               'responsibilities': [{'full_name': 'Personale: '
                                                  'ansættelse/afskedigelse'},
                                    {'full_name': 'Personale: Sygefravær'},
                                    {'full_name': 'Ansvar for bygninger og '
                                                  'arealer'}]}]}]"
    """

    graphql_query = gql(
        """
    query FindManagers($org_unit_uuid: [UUID!]) {
      managers(org_units: $org_unit_uuid) {
        objects {
          responsibilities {
            full_name
          }
          employee {
            name
            addresses {
              address_type {
                scope
              }
              name
            }
          }
        }
      }
    }
      """
    )
    response = gql_session.execute(
        graphql_query, variable_values={"org_unit_uuid": org_uuid}
    )

    return response["managers"]

#
# Copyright (c) Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import logging
from urllib.parse import urljoin

from os2mo_data_import.mox_data_types import (
    Organisation,
    Klassifikation,
    Facet,
    Klasse,
    Itsystem,
)
from os2mo_data_import.utilities import ImportUtility

logger = logging.getLogger("moImporterUtilities")


class CachingImportUtility(ImportUtility):
    """
    ImportUtility to automatically try to resolve existing import scaffolding,
    instead of reimporting
    """

    def __init__(
        self,
        mox_base,
        mora_base,
        demand_consistent_uuids,
        dry_run=False,
    ):
        # Global validity
        self.date_from = "1930-01-01"
        self.date_to = "infinity"

        super().__init__(
            mox_base,
            mora_base,
            demand_consistent_uuids,
            dry_run,
        )

    def _get_from_mox(self, resource, params):
        service_url = urljoin(base=self.mox_base, url=resource)
        r = self.session.get(service_url, params=params)
        r.raise_for_status()
        results = r.json()['results'][0]
        if len(results) > 1:
            raise ValueError(
                'More than one result found on resource {} with params {}'.format(
                    resource, params
                )
            )
        return results

    def import_organisation(self, reference, organisation: Organisation):
        resource = "organisation/organisation"

        r = self._get_from_mox(resource, params={'bvn': organisation.user_key})
        if r:
            org_uuid = r[0]
            self.organisation_uuid = org_uuid
            return org_uuid
        else:
            return super().import_organisation(reference, organisation)

    def import_klassifikation(self, reference, klassifikation: Klassifikation):
        resource = "klassifikation/klassifikation"
        r = self._get_from_mox(resource, params={'bvn': klassifikation.user_key})
        if r:
            klassifikation_uuid = r[0]
            self.klassifikation_uuid = klassifikation_uuid
            return klassifikation_uuid
        else:
            return super().import_klassifikation(reference, klassifikation)

    def import_facet(self, reference, facet: Facet):
        resource = "klassifikation/facet"
        r = self._get_from_mox(resource, params={'bvn': facet.user_key})
        if r:
            facet_uuid = r[0]
            self.inserted_facet_map[reference] = facet_uuid
            return facet_uuid
        else:
            return super().import_facet(reference, facet)

    def import_klasse(self, reference, klasse: Klasse):
        resource = "klassifikation/klasse"
        r = self._get_from_mox(resource, params={'bvn': klasse.user_key})
        if r:
            klasse_uuid = r[0]
            self.inserted_klasse_map[reference] = klasse_uuid
            return klasse_uuid
        else:
            return super().import_klasse(reference, klasse)

    def import_itsystem(self, reference, itsystem: Itsystem):
        resource = 'organisation/itsystem'
        r = self._get_from_mox(resource, params={'bvn': itsystem.user_key})
        if r:
            itsystem_uuid = r[0]
            self.inserted_itsystem_map[reference] = itsystem_uuid
            return itsystem_uuid
        else:
            return super().import_itsystem(reference, itsystem)

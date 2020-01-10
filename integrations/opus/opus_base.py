import json
import logging
import pathlib
import requests

from integrations.opus import opus_helpers

class OpusBase(object):
    def __init__(self, importer):
        cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
        print(cfg_file)
        if not cfg_file.is_file():
            raise Exception('No setting file')
        self.settings = json.loads(cfg_file.read_text())

        self.importer = importer
        self.importer.add_organisation(
            identifier=self.settings['municipality.name'],
            user_key=self.settings['municipality.name'],
            municipality_code=self.settings['municipality.code']
        )

        importer.new_itsystem(
            identifier='Opus',
            system_name='Opus'
        )

        self._add_klasse('AddressPostUnit', 'Postadresse',
                         'org_unit_address_type', 'DAR')
        self._add_klasse('Pnummer', 'Pnummer',
                         'org_unit_address_type', 'PNUMBER')
        self._add_klasse('EAN', 'EAN', 'org_unit_address_type', 'EAN')
        self._add_klasse('PhoneUnit', 'Telefon', 'org_unit_address_type', 'PHONE')
        self._add_klasse('PhoneEmployee', 'Telefon', 'employee_address_type',
                         'PHONE')
        self._add_klasse('EmailEmployee', 'Email',
                         'employee_address_type', 'EMAIL')
        self._add_klasse('CVR', 'CVR', 'org_unit_address_type')
        self._add_klasse('SE', 'SE', 'org_unit_address_type')
        self._add_klasse('AdressePostEmployee', 'Postadresse',
                         'employee_address_type', 'DAR')
        self._add_klasse('Lederansvar', 'Lederansvar', 'responsibility')
        self._add_klasse('Ekstern', 'Må vises eksternt', 'visibility', 'PUBLIC')
        self._add_klasse('Intern', 'Må vises internt', 'visibility', 'INTERNAL')
        self._add_klasse('Hemmelig', 'Hemmelig', 'visibility', 'SECRET')

    def _add_klasse(self, klasse_id, klasse, facet, scope='TEXT'):
        if not self.importer.check_if_exists('klasse', klasse_id):
            uuid = opus_helpers.generate_uuid(klasse_id)
            self.importer.add_klasse(
                identifier=klasse_id,
                uuid=uuid,
                facet_type_ref=facet,
                user_key=klasse,
                scope=scope,
                title=klasse
            )
        return klasse_id

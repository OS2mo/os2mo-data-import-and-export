import json
import requests
import click

from ra_utils.load_settings import load_settings
from pathlib import Path
from os2mo_helpers.mora_helpers import MoraHelper


# Todo: Add logger

class TestOpusConnectivity(object):
    def __init__(self):
        self.settings = load_settings()

        self.helper = MoraHelper(hostname=self.settings['mora.base'],
                                 use_cache=False)

    def _test_saml(self):
        print('Tester adgang til MO (især SAML)')
        try:
            self.helper.read_organisation()
            print(' * Kan tilgå MO uden problemer')
            print()
        except requests.exceptions.RequestException:
            print(' * Har ikke adgang til MO, check SAML eller hostnavn')
            exit(1)

    def _check_bare_minimum_keys(self):
        print('Tjeck for tilstedeværelse af konfigurationsnøgler')
        needed_keys = [
            'mora.base',
            'municipality.name',
            'integrations.opus.import.run_db',
            'integrations.opus.import.xml_path',
            'integrations.opus.eng_types_primary_order'
        ]

        missing_keys = []
        for key in needed_keys:
            if self.settings.get(key) is None:
                missing_keys.append(key)
        if missing_keys:
            print(' * Manglende nøgler: {}'.format(missing_keys))
            exit(1)
        else:
            print(' * Alle nødvendige nøgler fundet')
            print()

    def _check_xml_files_availability(self):
        print('Tjeck der findes xml-filer at importere')
        # Import will fail non-gracefully if keys are missing, hold import
        # until bare_minimum_check is performed
        from integrations.opus import opus_helpers
        dumps = opus_helpers.read_available_dumps()
        if len(dumps) == 0:
            msg = ' * Fandt ingen xml-filer i {}'
            print(msg.format(self.settings['integrations.opus.import.xml_path']))
            exit(1)
        else:
            newest_dump = sorted(dumps)[-1]
            msg = ' * Fandt {}-xml filer. Nyeste fil fra {}'
            print(msg.format(len(dumps), newest_dump))
            print()

    def _check_keys_for_diff_import(self):
        print('Tjeck for tilstedeværelse af nøgler for diff-import')
        print('Disse klasser oprettes af initial-import')
        diff_keys = [
            'mox.base',
            'opus.addresses.employee.dar',
            'opus.addresses.employee.phone',
            'opus.addresses.employee.email',
            'opus.addresses.unit.se',
            'opus.addresses.unit.cvr',
            'opus.addresses.unit.ean',
            'opus.addresses.unit.pnr',
            'opus.addresses.unit.phoneNumber',
            'opus.addresses.unit.dar',
            'integrations.opus.it_systems.ad',
            'integrations.opus.it_systems.opus'
        ]

        missing_keys = []
        for key in diff_keys:
            if self.settings.get(key) is None:
                missing_keys.append(key)
        if missing_keys:
            print(' * Manglende nøgler: {}'.format(missing_keys))
            exit(1)
        else:
            print(' * Alle nøgler for diff-import er fundet')
            print()

    def _check_lora_klasse(self, uuid, facet=None):
        url = '/klassifikation/klasse/'
        mox = self.settings['mox.base']
        response = requests.get(mox + url + uuid)
        if not response.status_code == 200:
            return False

        if facet:
            klasse = response.json()
            facet_uuid = (klasse[uuid][0]['registreringer'][0]['relationer']
                          ['facet'][0]['uuid'])
            url = '/klassifikation/facet/'
            facet_response = requests.get(mox + url + facet_uuid)
            raw_facet = facet_response.json()
            facet_titel = (
                raw_facet[facet_uuid][0]['registreringer'][0]['attributter']
                ['facetegenskaber'][0]['brugervendtnoegle']
            )
            if not facet_titel == facet:
                return False
        return True

    def _check_existense_of_base_klasser(self):
        print('Tjekker at engagements-typer for primærberegning eksisterer')
        bad_classes = []
        if not self.settings['integrations.opus.eng_types_primary_order']:
            print(' * Listen over primær-klasser er tom')
            exit(1)

        for uuid in self.settings['integrations.opus.eng_types_primary_order']:
            if not self._check_lora_klasse(uuid, facet='engagement_type'):
                bad_classes.append(uuid)

        if bad_classes:
            msg = ' * Klasse eksisterer ikke, eller ikke facet engagement_type: {}'
            print(msg.format(bad_classes))
            exit(1)
        else:
            print('* De angivne klasser til primærberegning er gyldige')
            print()

    def _check_legal_diff_klasser(self):
        print('Tjekker at de angivne adresseklasser eksisterer')
        bad_classes = []
        for uuid in [
                self.settings['opus.addresses.employee.dar'],
                self.settings['opus.addresses.employee.phone'],
                self.settings['opus.addresses.employee.email']
        ]:
            if not self._check_lora_klasse(uuid, facet='employee_address_type'):
                bad_classes.append(uuid)

        for uuid in [
                self.settings['opus.addresses.unit.se'],
                self.settings['opus.addresses.unit.cvr'],
                self.settings['opus.addresses.unit.ean'],
                self.settings['opus.addresses.unit.pnr'],
                self.settings['opus.addresses.unit.phoneNumber'],
                self.settings['opus.addresses.unit.dar']
        ]:
            if not self._check_lora_klasse(uuid, facet='org_unit_address_type'):
                bad_classes.append(uuid)

        if bad_classes:
            msg = ' * Klasse eksisterer ikke, eller har forkert facet: {}'
            print(msg.format(bad_classes))
            exit(1)
        else:
            print('* De angivne adresseklasser er gyldige')
            print()

    def _check_run_db(self, should_be_empty: bool):
        run_db = Path(self.settings['integrations.opus.import.run_db'])
        existing_run_db = run_db.is_file()

        if should_be_empty:
            print('Dette er førstegangsimport, tjek at run-db ikke findes:')
            if existing_run_db:
                print('run-db eksisterer! Skal fjernes før førstegangsimport')
                exit(1)
            else:
                print('Der findes (korrekt) ikke nogen run-db')

        else:
            print('Dette er diff-import, tjek at run-db findes:')
            if not existing_run_db:
                print('run-db eksisterer ikke! Udfør førstegangsimport!')
                exit(1)
            else:
                print(' * run-db findes som den skal')
                print()

    def _check_is_systems(self):
        print('Tjekker at opsætning af IT-systemer er korrekt')
        mo_it_systems = self.helper.read_it_systems()

        # Check opus
        found_opus = None
        opus = self.settings['integrations.opus.it_systems.opus']
        for it_system in mo_it_systems:
            if it_system['uuid'] == opus:
                found_opus = it_system['name']

        if not found_opus:
            msg = ' * Fandt ikke IT-system Opus med uuid: {}'
            print(msg.format(opus))
            exit(1)
        else:
            if found_opus.lower().find('opus') > -1:
                msg = ' * Fandt korrekt IT system til Opus: {}'
            else:
                msg = ' * Fandt IT system til Opus, men med navnet {}????'
            print(msg.format(found_opus))

        # Check ad
        found_ad = None
        ad = self.settings['integrations.opus.it_systems.ad']
        for it_system in mo_it_systems:
            if it_system['uuid'] == ad:
                found_ad = it_system['name']

        if not found_ad:
            msg = ' * Fandt ikke IT-system AD med uuid: {}'
            print(msg.format(ad))
            exit(1)
        else:
            ad_names = ['ad', 'active directory']
            name_matched = False
            for name in ad_names:
                if found_ad.lower().find(name) > -1:
                    name_matched = True
            if name_matched:
                msg = ' * Fandt korrekt IT system til AD: {}'
            else:
                msg = ' * Fandt IT system til AD, men med navnet {}????'
            print(msg.format(found_ad))
            print()

    def base_opus_check(self):
        self._test_saml()
        self._check_bare_minimum_keys()
        self._check_xml_files_availability()
        self._check_existense_of_base_klasser()

    def diff_opus_check(self):
        self._check_keys_for_diff_import()
        self._check_legal_diff_klasser()
        self._check_is_systems()
        self._check_run_db(should_be_empty=False)


@click.command(help="Test Opus configuration")
@click.option("--test-diff-import", is_flag=True)
def cli(**args):
    toc = TestOpusConnectivity()
    toc.base_opus_check()
    if args['test_diff_import']:
        toc.diff_opus_check()
    else:
        toc._check_run_db(should_be_empty=True)


if __name__ == '__main__':
    cli()

import click
import logging
import pathlib
from uuid import UUID
from integrations.SD_Lon.sd_common import sd_lookup
from integrations.SD_Lon.sd_common import load_settings


LOG_LEVEL = logging.DEBUG
LOG_FILE = "test_sd_connectivity.log"

def setup_logging():
    detail_logging = ("sdCommon")
    for name in logging.root.manager.loggerDict:
        if name in detail_logging:
            logging.getLogger(name).setLevel(LOG_LEVEL)
        else:
            logging.getLogger(name).setLevel(logging.ERROR)

    logging.basicConfig(
        format="%(levelname)s %(asctime)s %(name)s %(message)s",
        level=LOG_LEVEL,
        filename=LOG_FILE,
    )


class TestSdConnectivity(object):
    def __init__(self):
        try:
            self.settings = load_settings()
        except Exception as e:
            print('Syntax error in settings file: {}'.format(e))
            exit(1)

    def _check_existens_of_keys(self):
        print('Check for tilstedeværlse af konfigurationsøgler')
        needed_keys = [
            'integrations.SD_Lon.sd_user',
            'integrations.SD_Lon.sd_password',
            'integrations.SD_Lon.institution_identifier',
            'integrations.SD_Lon.import.too_deep',
            'integrations.SD_Lon.global_from_date',
            'integrations.SD_Lon.monthly_hourly_divide',
            'integrations.SD_Lon.job_function',
            'integrations.SD_Lon.import.run_db'
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

        nice_to_have_keys = [
            'integrations.SD_Lon.employment_field',
            'integrations.SD_Lon.import.manager_file',
            'integrations.SD_Lon.skip_employment_types'
        ]
        wanted_keys = []
        for key in nice_to_have_keys:
            if self.settings.get(key) is None:
                wanted_keys.append(key)
        if wanted_keys:
            print('Disse ikke-kritiske nøgler mangler: {}'.format(missing_keys))
        else:
            print(' * Alle ikke-kritiske nøgler fundet')
        print()

    def _check_legal_values(self):
        print('Tjekker at settings-værdier er gyldige:')

        legal_job_functions = ['EmploymentName', 'JobPositionIdentifier']
        legal_extensions = [
            'extension_1', 'extension_2', 'extension_3', 'extension_4',
            'extension_5', 'extension_6', 'extension_7', 'extension_8',
            'extension_9', 'extension_10'
        ]

        job_function = self.settings['integrations.SD_Lon.job_function']
        if job_function in legal_job_functions:
            print(' * Job function nøgle er korrekt')
        else:
            print(' * Job function nøgle skal være{}'.format(legal_job_functions))
            exit(1)

        employment_field = self.settings.get('integrations.SD_Lon.employment_field')
        if employment_field is None:
            print(' * Der skrives ikke stillingsbetegnelse til udvidelsesfelt')
        else:
            if employment_field in legal_extensions:
                print(' * Udvidelsesfelt til stillingsbetegnelse er korrekt')
            else:
                msg = ' * Udvidelsesfelt til stillingsbetegnelse skal være {}'
                print(msg.format(legal_extensions))
                exit(1)
        print()

        print('Tjekker indstillinger for ledere')
        manager_file = self.settings.get('integrations.SD_Lon.import.manager_file')
        if manager_file is not None:
            manager_path = pathlib.Path(manager_file)
            if manager_path.is_file():
                print(' * Specificeret lederfil er fundet')
            else:
                print(' * Specificeret lederfil er ikke fundet')
                exit(1)
            if manager_path.suffix == '.csv':
                print(' * Specificeret lederfil er korrekt en csv-fil')
            else:
                print(' * Specificeret lederfil skal være en csv-fil')
                exit(1)
        else:
            print(' * Der er ikke specificeret en lederfil')
        print()

        skip_job_functions = self.settings.get(
            'integrations.SD_Lon.skip_employment_types'
        )
        if skip_job_functions is not None:
            if not isinstance(skip_job_functions, list):
                print('skip_employment_types skal være en liste')
                exit(1)
            for job_function in skip_job_functions:
                try:
                    int(job_function)
                except ValueError:
                    print('All elementer i skip_employment_types skal være tal')
                    exit(1)
            print(' * skip_employment_types er korrekt')

    def _check_contact_to_sd(self):
        print('Tjekker at vi har kontakt til SD:')
        inst_id = self.settings['integrations.SD_Lon.institution_identifier']
        params = {
            'UUIDIndicator': 'true',
            'InstitutionIdentifier': inst_id
        }
        try:
            institution_info = sd_lookup(
                'GetInstitution20111201', params, use_cache=False
            )
        except Exception as e:
            print('Fejl i kontakt til SD Løn: {}'.format(e))
            exit(1)

        try:
            institution = institution_info['Region']['Institution']
            institution_uuid = institution['InstitutionUUIDIdentifier']
            UUID(institution_uuid, version=4)
            print(' * Korrekt kontakt til SD Løn')
        except Exception as e:
            msg = ' * Fik forbindelse, men modtog ikke-korrekt svar fra SD: {}, {}'
            print(msg.format(institution_uuid, e))
            exit(1)

    def sd_check(self):
        self._check_existens_of_keys()
        self._check_legal_values()
        self._check_contact_to_sd()


@click.command()
def check_connectivity():
    """Check SD configuration and connectivity."""
    tsc = TestSdConnectivity()
    tsc.sd_check()


if __name__ == '__main__':
    setup_logging()
    check_connectivity()

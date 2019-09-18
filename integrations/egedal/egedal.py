import os
import sys
import datetime

from os2mo_data_import import ImportHelper
sys.path.append('../SD_Lon')
import sd_importer

MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'SD-Løn Import')
MUNICIPALTY_CODE = os.environ.get('MUNICIPALITY_CODE', 0)
MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')

GLOBAL_GET_DATE = datetime.datetime(2019, 6, 13, 0, 0)

# Import of Administration
adm_name = 'AdmOrg'

importer = ImportHelper(
    create_defaults=True,
    mox_base=MOX_BASE,
    mora_base=MORA_BASE,
    system_name='SD-Import',
    end_marker='SDSTOP',
    store_integration_data=True
)

importer.add_klasse(
    identifier=adm_name,
    facet_type_ref='org_unit_type',
    user_key=adm_name,
    scope='TEXT',
    title=adm_name
)

importer.add_organisation_unit(
    identifier=adm_name,
    name=adm_name,
    user_key=adm_name,
    type_ref=adm_name,
    date_from='1900-01-01',
    date_to=None,
    parent_ref=None
)

sd = sd_importer.SdImport(
    importer,
    MUNICIPALTY_NAME,
    MUNICIPALTY_CODE,
    import_date_from=GLOBAL_GET_DATE,
    org_only=True,
    org_id_prefix='adm_'
)

sd.create_ou_tree(
    create_orphan_container=False,
    # 'Direktionen encoded with org prefix 'adm_'
    sub_tree='fff9e2a6-d670-b656-c719-994eeac03a74',
    super_unit = adm_name
)


# Import of Lønorganisation
løn_name = 'LønOrg'


importer.add_klasse(
    identifier=løn_name,
    facet_type_ref='org_unit_type',
    user_key=løn_name,
    scope='TEXT',
    title=løn_name
)

importer.add_organisation_unit(
    identifier=løn_name,
    name=løn_name,
    user_key=løn_name,
    type_ref=løn_name,
    date_from='1900-01-01',
    date_to=None,
    parent_ref=None
)

sd = sd_importer.SdImport(
    importer,
    MUNICIPALTY_NAME,
    MUNICIPALTY_CODE,
    import_date_from=GLOBAL_GET_DATE,
    org_only=True,
    org_id_prefix=None
)

sd.create_ou_tree(
    create_orphan_container=False,
    super_unit = løn_name
)

importer.import_all()

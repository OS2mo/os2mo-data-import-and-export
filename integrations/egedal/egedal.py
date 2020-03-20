import json
import pathlib

from integrations import cpr_mapper
from os2mo_data_import import ImportHelper
from integrations.SD_Lon import sd_importer
# from integrations.ad_integration import ad_reader

# TODO: Soon we have done this 4 times. Should we make a small settings
# importer, that will also handle datatype for specicic keys?
cfg_file = pathlib.Path.cwd() / 'settings' / 'settings.json'
if not cfg_file.is_file():
    raise Exception('No setting file')
settings = json.loads(cfg_file.read_text())

cpr_map = pathlib.Path.cwd() / 'settings' / 'cpr_uuid_map.csv'
if not cpr_map.is_file():
    raise Exception('No mapping file')
employee_mapping = cpr_mapper.employee_mapper(str(cpr_map))

importer = ImportHelper(
    create_defaults=True,
    mox_base=settings['mox.base'],
    mora_base=settings['mora.base'],
    store_integration_data=False,
    seperate_names=True
)

ad_reader = None

# Import of Administration
adm_name = 'AdmOrg'

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
    date_from='1930-01-01',
    date_to=None,
    parent_ref=None
)

sd = sd_importer.SdImport(
    importer,
    org_only=True,
    org_id_prefix='adm_'
)

sd.create_ou_tree(
    create_orphan_container=False,
    # 'Direktionen encoded with org prefix 'adm_'
    sub_tree='fff9e2a6-d670-b656-c719-994eeac03a74',
    super_unit=adm_name
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
    date_from='1930-01-01',
    date_to=None,
    parent_ref=None
)

sd = sd_importer.SdImport(
    importer,
    org_only=False,
    org_id_prefix=None
)

sd.create_ou_tree(
    create_orphan_container=False,
    super_unit=løn_name
)

importer.import_all()

import os
import sys
from os2mo_data_import import ImportHelper
import viborg_uuids
sys.path.append('..')
import sd_importer

MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'SD-Løn Import')
MUNICIPALTY_CODE = os.environ.get('MUNICIPALITY_CODE', 0)
MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:8080')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')

importer = ImportHelper(
    create_defaults=True,
    mox_base=MOX_BASE,
    mora_base=MORA_BASE,
    system_name='SD-Import',
    end_marker='SDSTOP',
    store_integration_data=True
)

ad_usernames, ad_uuids = viborg_uuids.read_ad_and_uuids()

importer.new_itsystem(
    identifier='AD',
    system_name='Active Directory'
)

sd = sd_importer.SdImport(
    importer,
    MUNICIPALTY_NAME,
    MUNICIPALTY_CODE,
    forced_uuids=ad_uuids,
)

for cpr, username in ad_usernames.items():
    if importer.check_if_exists('employee', cpr):
        importer.join_itsystem(
            employee=cpr,
            user_key=username,
            itsystem_ref='AD',
            date_from=None
        )

sd.employee_forced_uuids = ad_uuids
sd.employee_ad_usernames = ad_usernames

sd.create_ou_tree()
sd.create_employees()

importer.import_all()

"""
for info in sd.address_errors.values():
    print(info['DepartmentName'])
    print(info['DepartmentIdentifier'])
    print(info['PostalAddress']['StandardAddressIdentifier'])
    print(info['PostalAddress']['PostalCode'] + ' ' +
          info['PostalAddress']['DistrictName'])
    print()
    print()
print(len(sd.address_errors))
"""

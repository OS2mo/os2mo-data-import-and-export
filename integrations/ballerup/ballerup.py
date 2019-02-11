#!/usr/bin/env python3
#
# Copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
import os
import sys
sys.path.append('..')
import apos_importer
from os2mo_data_import import ImportHelper
# from os2mo_data_import import ImportUtility

MUNICIPALTY_NAME = os.environ.get('MUNICIPALITY_NAME', 'APOS Import')
MOX_BASE = os.environ.get('MOX_BASE', 'http://localhost:5000')
MORA_BASE = os.environ.get('MORA_BASE', 'http://localhost:80')
MUNICIPALTY_CODE = os.environ.get('MUNICIPALITY_NAME', 0)

importer = ImportHelper(create_defaults=True,
                        mox_base=MOX_BASE,
                        mora_base=MORA_BASE,
                        system_name='APOS-Import',
                        end_marker='APOSSTOP',
                        store_integration_data=True)


apos_import = apos_importer.AposImport(importer,
                                       MUNICIPALTY_NAME,
                                       MUNICIPALTY_CODE)
apos_import.create_facetter_and_klasser()

apos_import.create_ou_tree('b78993bb-d67f-405f-acc0-27653bd8c116')
sd_enhedstype = '324b8c95-5ff9-439b-a49c-1a6a6bba4651'
apos_import.create_ou_tree('945bb286-9753-4f77-9082-a67a5d7bdbaf',
                           enhedstype=sd_enhedstype)
apos_import.create_managers_and_associatins()

#import_tool = ImportUtility(dry_run=False, mox_base=MOX_BASE,
#                            mora_base=MORA_BASE, store_integration_data=True,
#                            system_name='APOS Import')
#import_tool.import_all(apos_import.org)
importer.import_all()

print('********************************')
print('Address challenges:')
for challenge in apos_import.address_challenges:
    print(apos_import.address_challenges[challenge])
print()

print('Address Errors:')
for error in apos_import.address_errors:
    print(apos_import.address_errors[error])
print()

print('Klassifikation Errors:')
for uuid, error in apos_import.klassifikation_errors.items():
    print(uuid)
print()

print('Duplicate people:')
for uuid, person in apos_import.duplicate_persons.items():
    print(person['@adresseringsnavn'])

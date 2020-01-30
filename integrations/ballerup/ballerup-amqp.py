import os
import sys
import datetime
import pathlib
import json
from os2mo_data_import import ImportHelper
from integrations.SD_Lon import sd_importer

settings = json.loads(pathlib.Path("settings/settings.json").read_text())

importer = ImportHelper(
    create_defaults=True,
    mox_base=settings["mox.base"],
    mora_base=settings["mora.base"],
    system_name='SD-Import',
    end_marker='SDSTOP',
    store_integration_data=True,
    seperate_names=False
)


sd = sd_importer.SdImport(
    importer=importer,
    ad_info=None,
    org_only=True,
)

sd.create_ou_tree(create_orphan_container=True)
importer.import_all()

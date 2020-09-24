import json
import pathlib
import requests

from os2mo_helpers.mora_helpers import MoraHelper

from integrations.lazy_settings import get_settings
settings = get_settings()

helper = MoraHelper(hostname=settings['mora.base'])
ORG = helper.read_organisation()

log_file = pathlib.Path.cwd() / 'mo_integrations.log_initial_import'
log_text = log_file.read_text()

pos = 0
managers = []
double_managers = []
while pos > -1:
    pos = log_text.find('Manager ', pos + 1)
    end = log_text.find('\n', pos)
    manager = log_text[pos:end]
    if manager not in managers:
        managers.append(manager)
    else:
        double_managers.append(manager)


for manager in double_managers:
    cpr = manager[8:18]
    print(cpr)
    employee = helper.read_user(user_cpr=cpr)

    correct_objects = []
    delete_uuids = []
    manager_objects = helper._mo_lookup(employee['uuid'], 'e/{}/details/manager')
    for manager in manager_objects:
        if manager['org_unit'] not in correct_objects:
            correct_objects.append(manager['org_unit'])
        else:
            delete_uuids.append(manager['uuid'])

    for uuid in delete_uuids:
        mox_url = '/organisation/organisationfunktion/{}/'.format(uuid)
        response = requests.delete(settings['mox.base'] + mox_url)
        print(response.text)

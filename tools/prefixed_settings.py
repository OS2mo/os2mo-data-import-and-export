#copyright (c) 2017-2018, Magenta ApS
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# replace possibly sick jq-script - whoich I accuse of making big swap-files

import os
import json
import pathlib


def extract_prefixed_envvars(settings, prefix):
    if not prefix.endswith("."):
        prefix +="."
    environment=[]
    prefixlen=len(prefix)
    for k,v in settings.items():
        if (
            k.startswith(prefix) and isinstance(v, str) 
            and k.count(".") == prefix.count(".")
        ):
            environment.append("export %s=\"%s\"" %(k[prefixlen:],v))
    return environment

if __name__ == '__main__':
    customer_settings_path = pathlib.Path(os.environ["CUSTOMER_SETTINGS"])
    prefix = os.environ["SETTING_PREFIX"]
    settings = json.loads(customer_settings_path.read_text())
    environment = extract_prefixed_envvars(settings, prefix)
    print ("\n".join(environment))

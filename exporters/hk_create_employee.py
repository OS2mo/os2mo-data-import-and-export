import json
import os
import queue
import threading
import time
import logging
import codecs

import click
import requests
from anytree import Node, PreOrderIter
from datetime import datetime, timedelta


from os2mo_helpers.mora_helpers import MoraHelper
from os2mo_data_import import ImportHelper

import holstebro_helpers as hh


@click.command()
@click.option('--dev', 'hostname', flag_value='https://os2mo-dev.holstebro.dk', help='https://os2mo-dev.holstebro.dk')
@click.option('--test', 'hostname', flag_value='https://os2mo-test.holstebro.dk', help='https://os2mo-test.holstebro.dk')
@click.option('--prod', 'hostname', flag_value='https://os2mo.holstebro.dk', help='https://os2mo.holstebro.dk')
@click.option('--api_token', envvar='SAML_TOKEN', default=None, required=True, help='SAML API Token')
@click.option('--employeeinfo', default=None, required=True, help='json formatted file with employee information')
def create_employee(hostname, api_token, employeeinfo):

    mh = MoraHelper(hostname, False, False)
    hk_helper = hh.HolstebroHelper(mh)

    with codecs.open(employeeinfo, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    uuid = hk_helper.add_employee(data)
    print(f"Employee with uuid: {uuid} has been created.")


if __name__ == "__main__":
    create_employee()

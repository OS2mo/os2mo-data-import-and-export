# -- coding: utf-8 --

import json
import logging
from requests import Session
from collections import OrderedDict

LOG = logging.getLogger(__name__)


def get_request(url, **params):

    request = Session()

    response = request.get(url, params=params)

    if response.status_code != 200:
        print(response.text)
        raise ConnectionError("Get request failed")

    return response


def get_address(*args, **kwargs):
    return fuzzy_address(args, kwargs)


previously_received = {}


def fuzzy_address(address_string, zip_code, city):

    DAWA_URL = "https://dawa.aws.dk/adresser"

    create_id = "{adr}-{zip}".format(
        adr=address_string,
        zip=zip_code
    )

    params = {
        "url": DAWA_URL,
        "q": address_string,
        "postnr": zip_code,
        "fuzzy": "true"
    }

    try:
        adr_data = address_string.split(",")

        if len(adr_data) == 1:
            floor = {
                "etage": ""
            }

            params.update(floor)
    except Exception as error:
        LOG.error(
            "ERROR processing data: {}".format(address_string)
        )
        return

    response = get_request(**params)
    data = response.json()

    return data["id"]

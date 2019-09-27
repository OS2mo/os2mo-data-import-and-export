import os
import pickle
import logging
import hashlib
import requests
import xmltodict
from pathlib import Path
import json
logger = logging.getLogger("sdCommon")

CFG_PREFIX = "integrations.SD_Lon.sd_common."

def get_prefixed_configuration(cfg, prefix):
    return {
        k.replace(prefix,"") : v
        for k, v in cfg.items()
        if k.startswith(prefix)
    }



class SD:

    @classmethod
    def create(cls, config_path):
        "constructor with config path"
        with open(config.path) as cfg:
            config = json.load(cfg)
            return cls(**{
                k.replace(pfix,"") : v
                for k, v in config.items()
                if k.startswith(pfix)
            })

    def __init__(self, **kwargs):
        cfg = self.config = kwargs
        try:
            self.institution_identifier = cfg["INSTITUTION_IDENTIFIER"]
            self.sd_user = cfg["SD_USER"]
            self.sd_password = cfg["SD_PASSWORD"]
            self.base_url = cfg["BASE_URL"]
        except:
            raise Exception('Credentials missing')

    def lookup(self, url, params={}):
        logger.info('Retrive: {}'.format(url))
        logger.debug('Params: {}'.format(params))

        full_url = self.base_url + url

        payload = {
            'InstitutionIdentifier': self.institution_identifier,
        }
        payload.update(params)
        m = hashlib.sha256()

        keys = sorted(payload.keys())
        for key in keys:
            m.update((str(key) + str(payload[key])).encode())
        m.update(full_url.encode())
        lookup_id = m.hexdigest()
        cache_file = Path('sd_' + lookup_id + '.p')

        if cache_file.is_file():
            with open(str(cache_file), 'rb') as f:
                response = pickle.load(f)
            logger.info('This SD lookup was found in cache: {}'.format(lookup_id))
        else:
            response = requests.get(
                full_url,
                params=payload,
                auth=(self.sd_user, self.sd_password)
            )
#            with open(str(cache_file), 'wb') as f:
#                pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

        dict_response = xmltodict.parse(response.text)
        if url in dict_response:
            xml_response = dict_response[url]
        else:
            logger.error('Envelope: {}'.format(dict_response['Envelope']))
            xml_response = {}
        logger.debug('Done with {}'.format(url))
        return xml_response


def calc_employment_id(employment):
    employment_id = employment['EmploymentIdentifier']
    try:
        employment_number = int(employment_id)
    except ValueError:  # Job id is not a number?
        employment_number = 999999

    employment_id = {
        'id': employment_id,
        'value': employment_number
    }
    return employment_id

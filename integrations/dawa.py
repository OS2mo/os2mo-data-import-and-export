import pickle
import requests


def _dawa_request(street_name, postal_code, adgangsadresse=False,
                  skip_letters=False, add_letter=False):
    """
    Heper function to perform a request to DAWA and return the json object.
    :param streetname: Address street name.
    :param postal_code: Postal code part of the address.
    :param adgangsadresse: If true, search for adgangsadresser.
    :param skip_letters: If true, remove letters from the house number.
    :return: The DAWA json object as a dictionary.
    """
    if adgangsadresse:
        base = 'https://dawa.aws.dk/adgangsadresser?'
    else:
        base = 'https://dawa.aws.dk/adresser?strukur=mini'
    params = '&postnr={}&q={}'

    last_is_letter = (street_name[-1].isalpha() and
                      (not street_name[-2].isalpha()))
    if (skip_letters and last_is_letter):
        street_name = street_name[:-1]
    full_url = base + params.format(postal_code, street_name)
    path_url = full_url.replace('/', '_')

    try:
        with open(path_url + '.p', 'rb') as f:
            response = pickle.load(f)
    except FileNotFoundError:
        response = requests.get(full_url)
        with open(path_url + '.p', 'wb') as f:
            pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

    dar_data = response.json()
    return dar_data


def dawa_lookup(self, street_name, postal_code):
    """
    Lookup an address object in DAWA and try to find an UUID for the address. Various
    attempts will be be done to find the address, first as an address, and if this
    fails a seach or access address (Adgangsadresse).
    :param address: APOS address object.
    :return: DAWA UUID for the address, or None if it is not uniquely found.
    """
    dar_uuid = None
    dar_data = _dawa_request(street_name, postal_code)

    if len(dar_data) == 0:
        # Found no hits, first attempt is to remove the letter
        # from the address
        dar_data = _dawa_request(street_name, postal_code, skip_letters=True,
                                 adgangsadresse=True)
        if len(dar_data) == 1:
            dar_uuid = dar_data[0]['id']

    elif len(dar_data) == 1:
        dar_uuid = dar_data[0]['id']

    else:
        # Multiple results typically means we have found an
        # adgangsadresse
        dar_data = _dawa_request(street_name, postal_code, adgangsadresse=True)
        if len(dar_data) == 1:
            dar_uuid = dar_data[0]['id']

    return dar_uuid

import requests
from datetime import datetime
from urllib.parse import urljoin


def _count(mox_base, at=None):

    if not at:
        at = datetime.strftime(datetime.now(), '%Y-%m-%d')

    counts = {}
    orgfunc = ('/organisation/organisationfunktion?virkningstid=' + at +
               '&gyldighed=Aktiv&funktionsnavn={}')
    unit = '/organisation/organisationenhed?virkningstid=' + at + '&gyldighed=Aktiv'
    user = '/organisation/bruger?virkningstid=' + at + '&bvn=%'

    url = urljoin(mox_base, orgfunc.format('Engagement'))
    response = requests.get(url)
    counts['engagement_count'] = len(response.json()['results'][0])

    url = urljoin(mox_base, orgfunc.format('Orlov'))
    response = requests.get(url)
    counts['leave_count'] = len(response.json()['results'][0])

    url = urljoin(mox_base, orgfunc.format('Rolle'))
    response = requests.get(url)
    counts['role_count'] = len(response.json()['results'][0])

    url = urljoin(mox_base, orgfunc.format('Leder'))
    response = requests.get(url)
    counts['manager_count'] = len(response.json()['results'][0])

    url = urljoin(mox_base, orgfunc.format('Tilknytning'))
    response = requests.get(url)
    counts['association_count'] = len(response.json()['results'][0])

    url = urljoin(mox_base, unit)
    response = requests.get(url)
    counts['unit_count'] = len(response.json()['results'][0])

    url = urljoin(mox_base, user)
    response = requests.get(url)
    counts['person_count'] = len(response.json()['results'][0])
    return counts

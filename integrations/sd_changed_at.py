import os
import pickle
import requests
import xmltodict
import datetime

INSTITUTION_IDENTIFIER = os.environ.get('INSTITUTION_IDENTIFIER')
SD_USER = os.environ.get('SD_USER', None)
SD_PASSWORD = os.environ.get('SD_PASSWORD', None)
if not (INSTITUTION_IDENTIFIER and SD_USER and SD_PASSWORD):
    raise Exception('Credentials missing')

BASE_URL = 'https://service.sd.dk/sdws/'

from_date = datetime.datetime(2019, 2, 15, 0, 0)
to_date = datetime.datetime(2019, 2, 25, 0, 0)

#from_date = datetime.datetime(2019, 2, 26, 0, 0)
#to_date = datetime.datetime(2019, 2, 27, 0, 0)

#from_date = datetime.datetime(2019, 2, 27, 0, 0)
#to_date = datetime.datetime(2019, 2, 29, 0, 0)

#from_date = datetime.datetime(2019, 2, 29, 0, 0)
#to_date = datetime.datetime(2019, 3, 15, 0, 0)


def _sd_lookup(url, from_date=None, to_date=None, params = {}):
    payload = {
        'InstitutionIdentifier': INSTITUTION_IDENTIFIER,
    }
    payload.update(params)

    full_url = BASE_URL + url
    url_id = url
    url_id += 'ActivationDate' + payload['ActivationDate']
    url_id += 'DeactivationDate' + payload['DeactivationDate']

    try:
        with open(url_id + '.p', 'rb') as f:
            response = pickle.load(f)
        print('CACHED')
    except FileNotFoundError:
        response = requests.get(
            full_url,
            params=payload,
            auth=(SD_USER, SD_PASSWORD)
        )
        with open(url_id + '.p', 'wb') as f:
            pickle.dump(response, f, pickle.HIGHEST_PROTOCOL)

    xml_response = xmltodict.parse(response.text)[url]
    return xml_response


def read_employment_changed():
    url = 'GetEmploymentChangedAtDate20111201'
    #params = {
    #    'ActivationDate': from_date.strftime('%d.%m.%Y'),
    #    'DeactivationDate': to_date.strftime('%d.%m.%Y'),
    #    'DepartmentIndicator': 'true',
    #    'EmploymentStatusIndicator': 'true',
    #    'ProfessionIndicator': 'true',
    #    'WorkingTimeIndicator': 'true',
    #    'UUIDIndicator': 'true'
    #}
    params = {
        'ActivationDate': from_date.strftime('%d.%m.%Y'),
        'DeactivationDate': to_date.strftime('%d.%m.%Y'),
        'StatusActiveIndicator': 'true',
        'DepartmentIndicator': 'true',
        'EmploymentStatusIndicator': 'true',
        'ProfessionIndicator': 'true',
        'WorkingTimeIndicator': 'true',
        'UUIDIndicator': 'true',
        'StatusPassiveIndicator': 'false',
        'SalaryAgreementIndicator': 'false',
        'SalaryCodeGroupIndicator': 'false'
    }
    response = _sd_lookup(url, from_date, to_date, params=params)

    #print(response.keys())
    #print(response['Person'][0])
    #print()
    #print(response['Person'][1])
    #print()
    #print(response['Person'][2])
    #print()
    #print(response['Person'][3])
    #print()
    #print(response['Person'][4])
    #print()
    #print(response['Person'][4].keys())
    #print()
    #print(response['Person'][4]['Employment'][0].keys())
    #print()
    #print(response['Person'][4]['Employment'][1].keys())
    #for person in response['Person']:
    #    print(person)
    #    print()
    return response['Person']


def read_person_changed():
    params = {
        'ActivationDate': from_date.strftime('%d.%m.%Y'),
        'DeactivationDate': to_date.strftime('%d.%m.%Y'),
        'StatusActiveIndicator': 'true',
        'StatusPassiveIndicator': 'false',
        'ContactInformationIndicator': 'false',
        'PostalAddressIndicator': 'false'
    }

    url = 'GetPersonChangedAtDate20111201'
    response = _sd_lookup(url, params=params)
    return response['Person']



if __name__ == '__main__':
    person_changed = read_person_changed()
    #print()
    employments_changed = read_employment_changed()

    print(len(person_changed))
    print(len(employments_changed))

    """
    for person in person_changed:
        cpr = person['PersonCivilRegistrationIdentifier']
        found_eng = False
        for eng in employments_changed:
            if cpr in eng['PersonCivilRegistrationIdentifier']:
                found_eng = True
        print(found_eng)
    """
    for person in person_changed:
        cpr = person['PersonCivilRegistrationIdentifier']
        print(person)
        for employment in employments_changed:
            if employment['PersonCivilRegistrationIdentifier'] == cpr:
                if not isinstance(employment['Employment'], list):
                    emp_list = [employment['Employment']]
                else:
                    emp_list = employment['Employment']
                for emp in emp_list:
                    for key, value in emp.items():
                        print('{}: {}'.format(key, value))
                print()

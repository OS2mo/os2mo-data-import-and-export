import csv


def employee_mapper(filename):
    employee_mapping = {}
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            cpr = row['cpr']
            mo_uuid = row['mo_uuid']
            employee_mapping[cpr] = mo_uuid
    return employee_mapping

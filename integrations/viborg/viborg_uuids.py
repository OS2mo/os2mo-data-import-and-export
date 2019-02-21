import csv
from uuid import UUID
from chardet.universaldetector import UniversalDetector


def read_ad_and_uuids():
    file_name = 'AD_Users_Kommune_ALL.csv'

    detector = UniversalDetector()
    with open(file_name, 'rb') as csvfile:
        for row in csvfile:
            detector.feed(row)
            if detector.done:
                break
    detector.close()
    encoding = detector.result['encoding']

    usernames = {}
    uuids = {}
    with open(file_name, encoding=encoding) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            if row['Enabled'] == 'True':
                pass
            else:
                pass
            cpr = row['xAttrCPR']

            usernames[cpr] = row['SamAccountName']
            uuids[cpr] = row['ObjectGUID']
            UUID(uuids[cpr], version=4)  # Fail if not a valid uuid
        
    return usernames, uuids

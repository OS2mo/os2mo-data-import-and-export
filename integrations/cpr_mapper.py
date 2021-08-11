import csv
from operator import itemgetter


def employee_mapper(filename:str) -> dict:
    """
    >>> import tempfile
    >>> with tempfile.NamedTemporaryFile('w', encoding='utf-8') as tmpfile:
    ...      _ = tmpfile.write('cpr;mo_uuid;ad_guid;sam_account_name\\n')
    ...      _ = tmpfile.write('0101701111;1034f987-555a-4183-bad1-2c5846117cee;;\\n')
    ...      _ = tmpfile.write('1103701234;a9077886-db39-400a-b3c4-8b73933790d2;;\\n')
    ...      tmpfile.flush()
    ...      expected = {
    ...         '0101701111': '1034f987-555a-4183-bad1-2c5846117cee',
    ...         '1103701234': 'a9077886-db39-400a-b3c4-8b73933790d2'
    ...      }
    ...      employee_mapper(tmpfile.name) == expected
    True
    """
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        return dict(map(itemgetter('cpr', 'mo_uuid'), reader))  # type: ignore

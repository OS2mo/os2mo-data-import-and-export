import json
from csv import DictReader
from io import StringIO
from unittest.mock import MagicMock
from uuid import uuid4

from ..main import build_model_map
from ..main import build_org_tree
from ..main import dump_csv
from ..main import load_ad_tree
from ..main import parse_distinguished_name


_TOP_UUID = uuid4()
_CHILD_UUID = uuid4()
_GRANDCHILD_UUID = uuid4()

_AD_TREE = {
    _TOP_UUID: parse_distinguished_name("OU=Niveau 1,DC=Kommune"),
    _CHILD_UUID: parse_distinguished_name("OU=Niveau 2,OU=Niveau 1,DC=Kommune"),
    _GRANDCHILD_UUID: parse_distinguished_name(
        "OU=Niveau 3,OU=Niveau 2,OU=Niveau 1,DC=Kommune"
    ),
}


def test_load_ad_tree():
    dn = "OU=Niveau 1,DC=Kommune"
    mock_settings = MagicMock()
    mock_ad_connection = MagicMock()
    mock_ad_connection.response_to_json = MagicMock(
        return_value=json.dumps(
            {
                "entries": [
                    {
                        "attributes": {
                            "objectGUID": "{%s}" % _TOP_UUID,
                            "distinguishedName": dn,
                        }
                    }
                ]
            }
        )
    )
    ad_tree = load_ad_tree(mock_settings, mock_ad_connection)
    assert ad_tree[_TOP_UUID] == parse_distinguished_name(dn)


def test_build_model_map_uses_org_unit_type_and_org_unit_level():
    org_unit_type_uuid = uuid4()
    org_unit_level_uuid = uuid4()
    model_map = build_model_map(_AD_TREE, org_unit_type_uuid, org_unit_level_uuid)
    mo_org_unit = model_map[_TOP_UUID]
    assert mo_org_unit.org_unit_type.uuid == org_unit_type_uuid
    assert mo_org_unit.org_unit_level.uuid == org_unit_level_uuid


def test_build_model_map():
    model_map = build_model_map(
        _AD_TREE, org_unit_level_uuid=uuid4(), org_unit_type_uuid=uuid4()
    )
    assert model_map[_TOP_UUID].parent is None
    assert model_map[_TOP_UUID].name == "Niveau 1"
    assert model_map[_CHILD_UUID].parent.uuid == _TOP_UUID
    assert model_map[_CHILD_UUID].name == "Niveau 2"
    assert model_map[_GRANDCHILD_UUID].parent.uuid == _CHILD_UUID
    assert model_map[_GRANDCHILD_UUID].name == "Niveau 3"


def test_build_org_tree():
    org_tree = build_org_tree(_AD_TREE)
    assert org_tree == {_TOP_UUID: {_CHILD_UUID: {_GRANDCHILD_UUID: {}}}}


def test_dump_csv():
    writable = StringIO()
    dump_csv(_AD_TREE, writable)
    writable.seek(0)
    for ad_item, csv_row in zip(_AD_TREE.items(), DictReader(writable, delimiter=";")):
        ad_guid, parsed_dn = ad_item
        assert csv_row["UUID"] == str(ad_guid)
        assert csv_row["DistinguishedName"] == parsed_dn.dn

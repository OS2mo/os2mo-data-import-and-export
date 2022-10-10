from uuid import uuid4

from ..main import build_model_map


def test_build_model_map_uses_org_unit_type_and_org_unit_level():
    ad_guid = uuid4()
    ad_tree = {ad_guid: ("Top", "Niveau 2", "Niveau 3")}
    org_unit_type_uuid = uuid4()
    org_unit_level_uuid = uuid4()
    model_map = build_model_map(ad_tree, org_unit_type_uuid, org_unit_level_uuid)
    mo_org_unit = model_map[ad_guid]
    assert mo_org_unit.org_unit_type.uuid == org_unit_type_uuid
    assert mo_org_unit.org_unit_level.uuid == org_unit_level_uuid

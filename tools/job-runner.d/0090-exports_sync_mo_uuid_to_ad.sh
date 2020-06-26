exports_sync_mo_uuid_to_ad(){
    BACK_UP_AND_TRUNCATE+=(sync_mo_uuid_to_ad.log)
    ${VENV}/bin/python3 ${DIPEXAR}/integrations/ad_integration/sync_mo_uuid_to_ad.py --sync-all
}


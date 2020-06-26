exports_actual_state_export(){
    # kører en test-kørsel
    BACK_UP_AND_TRUNCATE+=(sql_export.log)
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/sql_export/sql_export.py
}


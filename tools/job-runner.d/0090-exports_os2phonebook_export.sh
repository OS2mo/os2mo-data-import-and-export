exports_os2phonebook_export(){
    # kører en test-kørsel
    BACK_UP_AND_TRUNCATE+=(os2phonebook_export.log)
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/os2phonebook/os2phonebook_export.py sql-export
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/os2phonebook/os2phonebook_export.py generate-json
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/os2phonebook/os2phonebook_export.py transfer-json
}


exports_viborg_emus(){
    set -e
    echo running viborg_emus
    ${VENV}/bin/python3 exporters/emus/viborg_xml_emus_sftp.py
}


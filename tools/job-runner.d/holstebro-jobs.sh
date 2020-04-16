#!/bin/bash

imports_holstebro_ledere(){
    set -e
    echo running holstebro_decorate_leaders
    ${VENV}/bin/python3 exporters/holstebro_decorate_leaders.py --test
}

exports_holstebro(){
    set -e
    echo "running exports_holstebro"
    ${VENV}/bin/python3 exporters/holstebro.py --test
}

imports_holstebro_ledere()
exports_holstebro()
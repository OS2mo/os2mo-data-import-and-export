#!/bin/bash

# Execute this script with either --import or --update
export PYTHONPATH=$PWD:$PYTHONPATH
script_dir=$(cd "$(dirname "$0")" || exit; pwd)

python3 "$script_dir/brønderslev.py" "$1"

#!/bin/bash

export PYTHONPATH=$PWD:$PYTHONPATH
script_dir=$(cd "$(dirname "$0")" || exit; pwd)

python3 "$script_dir/brøndby.py"



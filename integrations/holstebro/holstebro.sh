export SETTINGS_FILE="kommune-holstebro.json"
export PYTHONPATH=$PWD:$PYTHONPATH
script_dir=$(cd $(dirname $0); pwd)

python3 "$script_dir/holstebro.py"

export SETTINGS_FILE="kommune-brøndby.json"
export PYTHONPATH=$PWD:$PYTHONPATH
script_dir=$(cd $(dirname $0); pwd)

python3 "$script_dir/bøndby.py"



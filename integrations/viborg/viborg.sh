export SETTINGS_FILE="kommune-viborg.json"
export PYTHONPATH=$PWD:$PYTHONPATH
script_dir=$(cd $(dirname $0); pwd)

python3 "$script_dir/viborg.py"
# python3 viborg_without_ad.py


# Execute this script with either --import or --update
export PYTHONPATH=$PWD:$PYTHONPATH
script_dir=$(cd $(dirname $0); pwd)

python3 "$script_dir/brønderslev.py" $1




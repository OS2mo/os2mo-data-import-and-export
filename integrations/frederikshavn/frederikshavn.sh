# Execute this script with either --import or --update
# --update not currently implemented...
export PYTHONPATH=$PWD:$PYTHONPATH
script_dir=$(cd $(dirname $0); pwd)

python3 "$script_dir/frederikshavn.py" $1



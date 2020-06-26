exports_plan2learn(){
    set -e
    echo "running exports_plan2learn"
    declare -a CSV_FILES=(
	bruger
	leder
	engagement
	organisation
	stillingskode
    )
    ${VENV}/bin/python3 ${DIPEXAR}/exporters/plan2learn/plan2learn.py --lora
    
    (
        # get OUT_DIR and EXPORTS_DIR
        SETTING_PREFIX="mora.folder" source ${DIPEXAR}/tools/prefixed_settings.sh
	[ -z "$query_export" ] && exit 1
	for f in "${CSV_FILES[@]}"
	do
	    ${VENV}/bin/python3 ${DIPEXAR}/exporters/plan2learn/ship_files.py \
		   ${query_export}/plan2learn_${f}.csv ${f}.csv
	done
    )
}


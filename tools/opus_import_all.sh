# Made Jørgen

declare -i count=0
for i in /opt/magenta/dataimport/opus/*.xml

do
    bash integrations/rebild/rebild.sh --update
    let count+=1
    [ "$(($count % 10))" = "0" ] && (
	cp settings/run_db.sqlite settings/run_db.sqlite_bak_${count}.sql
	pg_dump --data-only -Umox mox > rebild_id_${count}.sql
    )

done


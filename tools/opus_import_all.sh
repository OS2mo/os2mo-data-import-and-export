# Made by Jørgen

cd /home/rebild.lan/svc-os2moad/CRON/os2mo-data-import-and-export/
source /home/rebild.lan/svc-os2moad/CRON/os2mo-data-import-and-export/venv/bin/activate

declare -i count=0
for i in /opt/customer/*.xml

do
    bash integrations/rebild/rebild.sh --update
    let count+=1
    [ "$(($count % 10))" = "0" ] && (
	cp ../run_db.sqlite ../run_db.sqlite_bak_${count}.sql

	salt-call os2mo.create_db_snapshot installation_type=docker
	mv /opt/docker/os2mo/database_snapshot/os2mo_database.sql /opt/docker/os2mo/database_snapshot/os2mo_database_bak_${count}.sql
	# pg_dump --data-only -Umox mox > rebild_id_${count}.sql
    )

done


import psycopg2
import json
import yaml
import pathlib
import os


def clear_db_tables(user, dbname, host, password):
    conn = psycopg2.connect(
        user=user,
        dbname=dbname,
        host=host,
        password=password
    )
    cursor = conn.cursor()

    query = (
        "select relname from pg_class where relkind='r' " +
        "and relname !~ '^(pg_|sql_)';"
    )

    cursor.execute(query)
    for row in cursor.fetchall():
        query = 'truncate {} cascade;'.format(row[0])
        cursor.execute(query)
    conn.commit()

def clear_docker_mox_tables(conf):
    ack = os.environ["MOX_DB_MUST_REALLY_BE_EMPTIED_EVERY_DAY"]
    clear_db_tables(
        user=conf["environment"]["DB_USER"],
        dbname=conf["environment"]["DB_NAME"],
        host=conf["networks"]["default"]["ipv4_address"],
        password=conf["environment"]["DB_PASSWORD"],
    )

def clear_mox_tables(conf):
    ack = os.environ["MOX_DB_MUST_REALLY_BE_EMPTIED_EVERY_DAY"]
    clear_db_tables(
        user=conf["DB_USER"],
        dbname="mox",
        host=conf["DB_HOST"],
        password=conf["DB_PASSWORD"],
    )

if __name__ == '__main__':
    settingsfile = pathlib.Path(__file__).resolve().parent.parent / "settings" / "settings.json"
    lora_config_file = json.loads(settingsfile.read_text())["crontab.LORA_CONFIG"]
    if lora_config_file.endswith("docker-compose.yml"):
        clear_docker_mox_tables(
            yaml.safe_load(pathlib.Path(lora_config_file).read_text())["services"]["mox-db"])
    else:
        clear_mox_tables(json.loads(pathlib.Path(lora_config_file).read_text()))

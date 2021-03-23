import requests
from operator import itemgetter
from collections import Counter
from exporters.utils.load_settings import load_settings
import click

def check_relations(session, base, uuid):
    r = session.get(base+ f"organisation/organisationfunktion?vilkaarligrel={uuid}&list=true")
    r.raise_for_status()
    return r.json()['results']

def get_all_uuids(session, base, bvn):
    r = session.get(base + f"klassifikation/klasse?brugervendtnoegle={bvn}")
    return r.json()['results'][0]

def delete_class(session, base, uuid):
    r = session.delete(base + f"klassifikation/klasse/{uuid}")

def main(delete_dups):
    base = "http://localhost:8080/"
    settings = load_settings()
    session = requests.Session()
    r = session.get(base + "klassifikation/klasse?list=true")
    all_classes = r.json()['results'][0]
    all_ids = map(itemgetter('id'), all_classes)
    all_classes = list(map(lambda c: c['registreringer'][0]['attributter']['klasseegenskaber'][0]['brugervendtnoegle'].lower(), all_classes))
    class_map = dict(zip(all_classes, all_ids))
    ldupl = [i for i, cnt in Counter(all_classes).items() if cnt > 1]
    for dup in ldupl:
        uuids = get_all_uuids(session, base, dup.replace(' ', '+'))
        for uuid in uuids:
            assert uuid not in settings.values()
            rel = check_relations(session, base, uuid)
            if len(rel) > 0:
                print("There are relations to this uuid:" + rel)
            if delete_dups:
                delete_class(session, base, uuid)
            else:
                print(dup, uuid, rel)
    print("Done")

@click.command()
@click.option("--delete", type=click.BOOL, default=False,  is_flag=True, required=False, help="Remove any class that has dublicates")
def cli(delete):
    """Tool to help remove classes from MO that are dublicates. 
    
    This tool is written to help clean up engagement_types that had the same name, but with different casing.
    """
    main(delete)

if __name__ == "__main__":
    cli()
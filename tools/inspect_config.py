#!python3
"""
inspect configuration and report keys missing
compared to kommune-andeby.json
"""
import json
import os
import pathlib
import pprint

osdipex = pathlib.Path(__file__).parent.parent.resolve()
settings_file = os.environ.get(
    "CUSTOMER_SETTINGS", (osdipex / "settings" / "settings.json")
)

if __name__ == "__main__":
    try:
        with open(osdipex / "settings" / "kommune-andeby.json") as f:
            template = json.load(f)
        t_keys = set([k for k in template.keys() if not k.startswith("#")])
    except:  # noqa: E722
        print("syntax error in template")
        raise

    try:
        with open(settings_file) as f:
            settings = json.load(f)
        s_keys = set([k for k in settings.keys() if not k.startswith("#")])
    except:  # noqa: E722
        print("syntax error in settings")
        raise

    print("The following keys are in template but not in settings")
    pprint.pprint(t_keys - s_keys)

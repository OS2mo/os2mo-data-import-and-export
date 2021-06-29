venv/bin/flake8 integrations/os2sync
venv/bin/python -m doctest integrations/os2sync/lcdb_os2mo.py
venv/bin/python -m doctest integrations/os2sync/os2mo.py
# pip install rstcheck
venv/bin/rstcheck integrations/os2sync/readme.rst


**********************
Integration for Aarhus LOS
**********************

Introduction
============

This integration makes it possible to fetch and import organisation and
person data from Aarhus LOS. It is built specifically for Aarhus Kommune, as the
data format is a custom format defined by them. The format is based on OPUS, but
has been subject to further processing, and as such the original
OPUS importer is incompatible

The importer connects to an external FTP provided by Aarhus Kommune, to fetch delta
files.

The importer utilizes a single piece of state in the form of a file, containing the
date of the last import run. Essentially a minimal implementation of the run-db found
in other importers

Setup
=====

The integration requires minimal configuration outside of the common default values
found in the settings file:

* ``integrations.aarhus_los.ftp_url``: The url for the Aarhus Kommune FTP. Contains a
  default for the current FTP url.
* ``integrations.aarhus_los.ftp_user``: The FTP user
* ``integrations.aarhus_los.ftp_pass``: The FTP pass
* ``integrations.aarhus_los.state_file``: A location for a file containing state across
  different imports.

Usage
=====

The importer can be run with the following command:

```
python integrations/aarhus/los_import.py
```

The command currently takes no parameters.

The command will:

* Perform an initial import, of all preset classes and organisation objects
  if it determines it hasn't taken place yet
* Ensure a state file exists, containing the date of the last import.
* Connect to the AAK FTP and perform the necessaryan import of all 'klasse', 'it system',
  org unit and person objects, to bring the system up to date.
* Write a timestamp of a completed import into the state file

The command is designed to be idempotent, and can theoretically be run as often as is
deemed necessary.

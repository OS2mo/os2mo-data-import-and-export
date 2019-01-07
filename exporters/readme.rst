CSV Exporters
=============

This set of code allows to export the content of MO into a fixed set of csv-files
containing various sub-sets of the content of MO.

Installation
------------
The code contains a general exporter as well as code specific to various
municipalities, the municipality-specfic code should only be run, if you are
running MO from the corresponding municipality, since these exporters expects
data specific to theses places.

The general code can be run directly from the folder with no installation required.

Requirements
------------
The requirements are indicated in requirements.txt

Configuration
-------------
If MO is setup to use authentication, the exporter needs a valid service SAML token.
This is read from the environment variable SAML_TOKEN.



Exported data
-------------
The general exporter will produce the following data-files:

 * alle_lederfunktioner_os2mo.csv
 * alle-medarbejdere-stilling-email_os2mo.csv
 * org_incl-medarbejdere.csv
 * adm-org-incl-start-og-stopdata-og-enhedstyper-os2mo.csv
 * tilknytninger.csv

Please note that these exports contain the same personal details as MO itself, and thus it is important to have a very strict handling of the exported files.

Command line options
--------------------
general_export.py accepts two command line parameters:

--root: uuid for the root org to export. If this parameter is not given, the deepest available tree will be assumed.

--threaded-speedup: If set to True, the program will start a full multithreaded read of all employees in MO. On most systems this will be significantly faster, but will result in a higher server load and a longer delay before the first export is finished.

--hostname: Hostname for the MO instance. Defaults to localhost.

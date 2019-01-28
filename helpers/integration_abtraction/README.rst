Intergation abstraction
=======================

A small library to easy the use of the `integrations_data` field in LoRa.

The utility provides functionality to read and write fields from the
integration data stored with an object, while taking care of not overwriting
other keys stored by other integrations on the same object.

The utility also provides functionality to find objects based on their
integration data.


nstalling
----------

Install the integration_abstraction package as follows: ::

# Checkout the mora source repository
git clone https://github.com/OS2mo/os2mo-data-import-and-export

# Navigate to the local copy of the repository
cd /path/to/os2mo-data-import-and-export

# Install package with pip
pip install helpers/integration_abstraction


Usage
-----

Import the utility, eg: ::

  from integration_abstraction.integration_abstraction import IntegrationAbstraction

The tool takes parameters for `system_name` and `end_marker`. `system_name`
is the name of the key that will be used for the current session, the
utility will take of abstracting away the unerlying json-structure atully
stored in the integration_data field, and will only handle the values
associated with the chosen key.

`end_marker` is the value that is appeded to all values to ensure that it is
possible to uniquely find objects despite the fact that structured search
is not avaiable for integration data in LoRa. The value defaults to STOP. If
this word could potentially be bart of the actual stored value, another
`end_marker` should be chosen.

<!--
SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
SPDX-License-Identifier: MPL-2.0
-->

CHANGELOG
=========

2.6.1 - 2022-01-12
------------------

[#47911] Fix UUID to string bug

2.6.0 - 2022-01-12
------------------

[#46874] Added default calculate primary function

2.5.6 - 2022-01-10
------------------

[#47856] Add retry to OS2phonebook export

2.5.5 - 2022-01-10
------------------

[#47656] Fix bug that would create it-systems with random uuid usernames during opus-import

2.5.4 - 2022-01-06
------------------

[#xxxxx] Fixes a bug where opus reimport script writes to rundb

2.5.3 - 2022-01-06
------------------

[#47656] Add handeling of changing/terminating IT-system accounts to Opus-diff-import

2.5.2 - 2022-01-05
------------------

[#47734] Fix LoRaCache not properly exporting historic data when called from the CLI

2.5.1 - 2022-01-03
------------------

[#47247] Fixes for opus reimport script

2.5.0 - 2021-12-30
------------------

[#47241] Add script to reimport users or units from opus.

2.4.0 - 2021-12-22
------------------

[#47488] Add 'vacuum' command to remove_duplicates tool

2.3.0 - 2021-12-22
------------------

[#47488] Add script to remove duplicate registrations from user tables

2.2.5 - 2021-12-22
------------------

[#47581] Do not read 'USE_CACHED_LORACACHE' environment variable when using the LoRaCache CLI

2.2.4 - 2021-12-21
------------------

[#47646] Fix OS2sync LoRa cache crashing because of missing "is_primary" field

2.2.3 - 2021-12-21
------------------

[#47581] Chill with the cached LoRaCache

2.2.2 - 2021-12-21
------------------

[#47622] Loracache allow vacant association

2.2.1 - 2021-12-20
------------------

[#47632] OS2sync uses paginated calls to avoid timeouts

2.2.0 - 2021-12-17
------------------

[#47581] Add support for cached LoRaCache

2.1.3 - 2021-12-17
------------------

[#44668] Associate leave with engagement during SD import

2.1.2 - 2021-12-17
------------------

[#47457] Catch and log when a managerrole has no person attached.

2.1.1 - 2021-12-16
------------------

[#47581] Decrease LoRaCache results per page from 5000 to 1000 to avoid LoRa database timeout

2.1.0 - 2021-12-16
------------------

[#47571] Fix update-dipex.sh error by adjusting build-system in pyproject.toml

[#47267] New featureflags for customizing opus-import

2.0.3 - 2021-12-15
------------------

[#47581] LoRa cache fix params retrying

2.0.2 - 2021-12-08
------------------

[#46715] Send git version to Grafana Cloud on checkout

2.0.1 - 2021-12-08
------------------

[#46715] Trigger new release

2.0.0 - 2021-12-08
------------------

[#46715] Implement automatic versioning through autopub

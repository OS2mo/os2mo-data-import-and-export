<!--
SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
SPDX-License-Identifier: MPL-2.0
-->

CHANGELOG
=========

2.20.3 - 2022-03-10
-------------------

[#46874] Fix recalculate_primary

2.20.2 - 2022-03-10
-------------------

[#48939] Handle SD engagements inconsistent according to no-salary-minimum

2.20.1 - 2022-03-09
-------------------

[#48939] Do not create inconsistent external engagements

2.20.0 - 2022-03-09
-------------------

[#46894] Use SD Pydantic settings model in SD-changed-at

2.19.1 - 2022-03-09
-------------------

[#45401] AD_sync removes engagement info from users when the AD-account is removed and removes AD engagement info from engagements that are no longer primary

2.19.0 - 2022-03-07
-------------------

[#46715] Use poetry for dependency management

2.18.1 - 2022-02-23
-------------------

[#48809] Skip connecting it-system during Opus import if multiple entries exist in MO.

2.18.0 - 2022-02-17
-------------------

[#46874] Better recalculate reporting

2.17.0 - 2022-02-10
-------------------

[#46894] SD Pydantic config module

2.16.0 - 2022-02-10
-------------------

[#48067] Log when creating engagement is skipped due to wierd SD data

2.15.1 - 2022-02-07
-------------------

[#48132] Patch for opus_terminate_filtered script, so it won't delete users with unfiltered engagements in the past or future.

2.15.0 - 2022-02-04
-------------------

[#48132] Add check for users and engagements that should have been filtered from opus-import and a script to remove them.

2.14.0 - 2022-02-02
-------------------

[#48375] Handle inconsistent leave start date

2.13.0 - 2022-02-01
-------------------

[#48243] Flag to choose SD engagement start date during import

2.12.1 - 2022-01-27
-------------------

[#48202] Handle Nones within fix_department.py

2.12.0 - 2022-01-27
-------------------

[#48136] Adds new columns to "Viborg eksterne" report. Deprecate MO version and support only loracache version of the script.

2.11.0 - 2022-01-26
-------------------

[#46166] Adds timestamp as a field available for AD_writer

2.10.2 - 2022-01-25
-------------------

[#48083] Makes os2sync integration able to use keycloak authentication.

2.10.1 - 2022-01-24
-------------------

[#48067] Fix BC anniversary date problem

2.10.0 - 2022-01-20
-------------------

[#48050] Fix engagement termination date bug

2.9.1 - 2022-01-19
------------------

[#47241] Move ad-script and add documentation

2.9.0 - 2022-01-18
------------------

[#41612] SD_changed_at uses datetimes to allow running over any timespan, eg. several days or multiple times a day.

2.8.0 - 2022-01-17
------------------

[#47960] Introduce continuous deployment to Flux

2.7.2 - 2022-01-14
------------------

[#47734] Fix LoRaCache CLI flag

2.7.1 - 2022-01-14
------------------

[#47734] Add job for RUN_CACHE_HISTORIC_SKIP_PAST_LORACACHE 🥲

2.7.0 - 2022-01-12
------------------

[#47623] Add new dipex-job exports_ad_enddate_fixer write enddates of terminated users to AD.

2.6.2 - 2022-01-12
------------------

[#47755] Fix NTLM to always use first set of credentials for the winrm connections

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

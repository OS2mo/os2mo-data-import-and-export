#!/bin/bash
# Redmine #52888
. tools/job-runner.sh
. .venv/bin/activate

metacli ensure_user_exists BDO robot --uuid=cf879708-9416-4941-b632-ae258cbac488 --cpr=9112300001 --user_key=d1xxrpa
metacli ensure_user_exists RPA User --uuid=88352bbe-dde9-48bc-a3c7-964a092f6dea --cpr=9112300002 --user_key=d1rpauser
metacli ensure_user_exists d1robot-kyfleks '' --uuid=88817ac8-fccd-41e8-8498-3cc823c403b9 --cpr=9112300003 --user_key=d1robot-kyfleks
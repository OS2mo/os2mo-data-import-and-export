
#!/bin/bash
# SPDX-FileCopyrightText: Magenta ApS
# SPDX-License-Identifier: MPL-2.0

set -o nounset
set -o errexit
set -o pipefail

# Apply Alembic migrations
ALEMBIC_CONFIG=./sql_export/alembic.ini alembic upgrade head

# Run app
uvicorn --factory sql_export.main:create_app --host 0.0.0.0
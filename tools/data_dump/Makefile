SHELL=/bin/bash

# Env vars & defaults
MOX_DB_CONTAINER ?= mox_database
MOX_DB_NAME ?= mox
KEYCLOAK_DB_CONTAINER ?= keycloak_database
KEYCLOAK_DB_NAME ?= keycloak
DOCKER_BACKUP_DIR ?= /database_snapshot

# Backup ALL THE THINGS
.PHONY: backup
backup: backup-mox # backup-keycloak

# Restore ALL THE THINGS
.PHONY: restore
restore: restore-mox # restore-keycloak


# Backup one of the things
.PHONY: backup-mox
backup-mox: 
	@bash ./check_docker.sh $(MOX_DB_CONTAINER)
	@bash ./backup.sh $(MOX_DB_CONTAINER) $(MOX_DB_NAME) $(DOCKER_BACKUP_DIR)

.PHONY: backup-keycloak
backup-keycloak:
	@bash ./check_docker.sh $(KEYCLOAK_DB_CONTAINER)
	@bash ./backup.sh $(KEYCLOAK_DB_CONTAINER) $(KEYCLOAK_DB_NAME) $(DOCKER_BACKUP_DIR)

# Restore one of the things
.PHONY: restore-mox
restore-mox:
	@bash ./check_docker.sh $(MOX_DB_CONTAINER)
	@bash ./restore.sh $(MOX_DB_CONTAINER) $(MOX_DB_NAME) $(DOCKER_BACKUP_DIR)

.PHONY: restore-keycloak
restore-keycloak:
	@bash ./check_docker.sh $(KEYCLOAK_DB_CONTAINER)
	@bash ./restore.sh $(KEYCLOAK_DB_CONTAINER) $(KEYCLOAK_DB_NAME) $(DOCKER_BACKUP_DIR)

.PHONY: cleanup
cleanup:
	@bash ./cleanup.sh $(MOX_DB_CONTAINER) $(MOX_DB_NAME) 
#	@bash ./cleanup.sh $(KEYCLOAK_DB_CONTAINER) $(KEYCLOAK_DB_NAME)
#!/bin/bash

# Backup script for mrr, stg, dwh databases
# Run: bash backup.sh

# Setting

PG_CONTAINER="postgres_dwh"
PG_USER="dwh_user"
BACKUP_DIR="./backups"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
DATABASES=("mrr" "stg" "dwh")

# Creating backup folder if it doesn't exist
mkdir -p "$BACKUP_DIR"

echo "Backup started: $TIMESTAMP"

# Iterating over all databases
for DB in "${DATABASES[@]}"; do

    BACKUP_FILE="$BACKUP_DIR/${DB}_${TIMESTAMP}.sql"

    echo "[$(date +"%H:%M:%S")] Starting backup: $DB -> $BACKUP_FILE"

    # pg_dump inside the container -> save file to host
    docker exec "$PG_CONTAINER" pg_dump -U "$PG_USER" -d "$DB" > "$BACKUP_FILE"

    # Checking success
    if [ $? -eq 0 ]; then
        SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
        echo "[$(date +"%H:%M:%S")] $DB backup success — size: $SIZE"
    else
        echo "[$(date +"%H:%M:%S")] $DB backup failed"
    fi

done

echo "Backup finished: $(date +"%H:%M:%S")"
echo "Backup location: $BACKUP_DIR"
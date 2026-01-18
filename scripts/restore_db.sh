#!/bin/bash
# Database restore script
# Usage: ./restore_db.sh backup_file.sql.gz

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <backup_file.sql.gz>"
    echo "Available backups:"
    ls -lh /home/ihammoud/archives_collection/backups/archives_backup_*.sql.gz 2>/dev/null || echo "No backups found"
    exit 1
fi

BACKUP_FILE="$1"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "ERROR: Backup file not found: $BACKUP_FILE"
    exit 1
fi

echo "WARNING: This will overwrite the current database!"
echo "Backup file: $BACKUP_FILE"
read -p "Are you sure you want to continue? (yes/no): " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "Restore cancelled"
    exit 0
fi

echo "Restoring database from $BACKUP_FILE..."

# Drop and recreate database
docker compose exec -T db psql -U postgres -c "DROP DATABASE IF EXISTS archives;"
docker compose exec -T db psql -U postgres -c "CREATE DATABASE archives;"

# Restore from backup
gunzip -c "$BACKUP_FILE" | docker compose exec -T db psql -U postgres -d archives

echo "Database restored successfully!"

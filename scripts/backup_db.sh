#!/bin/bash
# Database backup script for production
# Add to crontab: 0 3 * * * /path/to/backup_db.sh

set -e

# Configuration
BACKUP_DIR="/home/ihammoud/archives_collection/backups"
RETENTION_DAYS=7
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/archives_backup_$DATE.sql.gz"

# Ensure backup directory exists
mkdir -p "$BACKUP_DIR"

# Create backup using docker compose
echo "Starting backup at $(date)"
docker compose exec -T db pg_dump -U postgres -d archives | gzip > "$BACKUP_FILE"

# Verify backup was created and has content
if [ -s "$BACKUP_FILE" ]; then
    echo "Backup created successfully: $BACKUP_FILE"
    echo "Backup size: $(du -h "$BACKUP_FILE" | cut -f1)"
else
    echo "ERROR: Backup file is empty or was not created"
    rm -f "$BACKUP_FILE"
    exit 1
fi

# Remove old backups
echo "Removing backups older than $RETENTION_DAYS days..."
find "$BACKUP_DIR" -name "archives_backup_*.sql.gz" -mtime +$RETENTION_DAYS -delete

# List remaining backups
echo "Current backups:"
ls -lh "$BACKUP_DIR"/archives_backup_*.sql.gz 2>/dev/null || echo "No backups found"

echo "Backup completed at $(date)"

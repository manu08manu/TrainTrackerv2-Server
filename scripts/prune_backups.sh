#!/bin/bash
# Deletes .bak files and old jar copies (traintracker.jar.prev etc.) older than 30 days from traintracker deploy/backup
LOG=/opt/traintracker/logs/prune_backups.log
{
echo "=== $(date -Is) ==="
find /opt/traintracker -maxdepth 1 \( -name "*.bak-*" -o -name "*.bak" -o -name "*.bak[0-9]*" -o -name "*.bak.*" -o -name "*.jar.*" \) -mtime +30 -print -delete
find /opt/traintracker/static -name "*.bak*" -mtime +30 -print -delete
find /home/opc -name "*.bak-*" -mtime +30 -print -delete
} >> "$LOG" 2>&1

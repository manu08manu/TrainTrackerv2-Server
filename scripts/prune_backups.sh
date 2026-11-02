#!/bin/bash
# Deletes .bak files older than 30 days from traintracker deploy/backup
# locations. Skips the deliberately-kept baseline archive.
LOG=/opt/traintracker/logs/prune_backups.log
{
echo "=== $(date -Is) ==="
find /opt/traintracker -maxdepth 1 \( -name "*.bak-*" -o -name "*.bak" -o -name "*.bak[0-9]*" -o -name "*.bak.*" \) -mtime +30 \
! -name "traintracker_review_20260707.tar.gz" -print -delete
find /home/opc -name "*.bak-*" -mtime +30 -print -delete
} >> "$LOG" 2>&1

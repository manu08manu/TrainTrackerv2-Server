#!/bin/bash
# Delete RDM files older than 7 days, but only if a newer file of the same type
# (same name with digits and spaces removed) arrived within the last 7 days.
dir=/srv/rdm/in
mins=$((8*1440))
cd "$dir" || exit 1
key() { printf '%s' "$1" | tr -d ' 0-9'; }
declare -A recent
while IFS= read -r -d '' f; do recent["$(key "$f")"]=1; done \
  < <(find . -maxdepth 1 -type f -mmin -$mins -printf '%P\0')
while IFS= read -r -d '' f; do
  if [ -n "${recent[$(key "$f")]}" ]; then
    echo "$(date '+%F %T') deleting $f"
    [ -z "$DRY" ] && rm -f -- "$f"
  fi
done < <(find . -maxdepth 1 -type f -mmin +$mins -printf '%P\0')

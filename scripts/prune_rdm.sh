#!/bin/bash
# Delete RDM files older than 8 days, but only if a newer file of the same type
# (same name with digits and spaces removed) arrived within the last 8 days.
# PIF files each cover a different timetable period, so their type is the validity
# window from the header line instead of the file name.
dir=/srv/rdm/in
mins=$((8*1440))
cd "$dir" || exit 1
key() {
  case "$1" in
    PIF*) printf 'PIF-%s' "$(zcat -f -- "$1" 2>/dev/null | head -n1 | cut -f5,6 | tr -d ' \t')" ;;
    *)    printf '%s' "$1" | tr -d ' 0-9' ;;
  esac
}
declare -A recent
while IFS= read -r -d '' f; do recent["$(key "$f")"]=1; done \
  < <(find . -maxdepth 1 -type f -mmin -$mins -printf '%P\0')
while IFS= read -r -d '' f; do
  if [ -n "${recent[$(key "$f")]}" ]; then
    echo "$(date '+%F %T') deleting $f"
    [ -z "$DRY" ] && rm -f -- "$f"
  fi
done < <(find . -maxdepth 1 -type f -mmin +$mins -printf '%P\0')

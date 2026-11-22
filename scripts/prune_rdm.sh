#!/bin/bash
# Delete RDM files older than 8 days, but only if a newer file of the same type
# (same name with digits and spaces removed) arrived within the last 8 days.
# PIF files each cover a different timetable period, so their type is the validity
# window from the header line instead of the file name.
dir=/srv/rdm/in
mins=$((8*1440))
cd "$dir" || exit 1

# SMART berth steps are a full snapshot each time, so keep only the newest file (by the timestamp in its
# name), once it has settled: not touched for 10 minutes and not empty. No 8-day wait for this type.
newest=$(ls -- berth-steps-*.csv 2>/dev/null | sort | tail -n1)
if [ -n "$newest" ] && [ -s "$newest" ] && [ -z "$(find . -maxdepth 1 -name "$newest" -mmin -10)" ]; then
  for f in berth-steps-*.csv; do
    [ "$f" = "$newest" ] && continue
    echo "$(date '+%F %T') deleting $f (older berth-steps)"
    [ -z "$DRY" ] && rm -f -- "$f"
  done
fi
key() {
  case "$1" in
    PIF*) printf 'PIF-%s' "$(zcat -f -- "$1" 2>/dev/null | head -n1 | cut -f5,6 | tr -d ' \t')" ;;
    *)    printf '%s' "$1" | tr -d ' 0-9' ;;
  esac
}
# PIF: each validity window is its own type. Keep only the newest settled copy per
# window straight away, and drop files whose window ended before today (while at
# least one still-valid PIF file remains). Header fields 5/6 = valid from/to (DD-MM-YYYY).
declare -A pif_newest pif_end
today=$(date +%Y%m%d)
live=0
while IFS= read -r f; do
  pif_newest["$(key "$f")"]=$f
  e=$(zcat -f -- "$f" 2>/dev/null | head -n1 | cut -f6 | awk '{split($1,d,"-"); print d[3] d[2] d[1]}')
  pif_end["$f"]=$e
  if [[ $e =~ ^[0-9]{8}$ ]] && [ "$e" -ge "$today" ]; then live=$((live+1)); fi
done < <(ls -- PIF* 2>/dev/null | sort)
while IFS= read -r f; do
  e=${pif_end[$f]}
  n=${pif_newest[$(key "$f")]}
  if [[ $e =~ ^[0-9]{8}$ ]] && [ "$e" -lt "$today" ] && [ "$live" -ge 1 ]; then
    echo "$(date '+%F %T') deleting $f (PIF window ended)"
    [ -z "$DRY" ] && rm -f -- "$f"
  elif [[ $e =~ ^[0-9]{8}$ ]] && [ "$f" != "$n" ] && [ -s "$n" ] && [ -z "$(find . -maxdepth 1 -name "$n" -mmin -10)" ]; then
    echo "$(date '+%F %T') deleting $f (older PIF for the same window)"
    [ -z "$DRY" ] && rm -f -- "$f"
  fi
done < <(ls -- PIF* 2>/dev/null | sort)
# NLC (CORPUS) is a full snapshot dated in its name: keep only the newest once settled.
nlc_newest=$(ls -- NLC*.xml.gz 2>/dev/null | sort | tail -n1)
if [ -n "$nlc_newest" ] && [ -s "$nlc_newest" ] && [ -z "$(find . -maxdepth 1 -name "$nlc_newest" -mmin -10)" ]; then
  for f in NLC*.xml.gz; do
    [ "$f" = "$nlc_newest" ] && continue
    echo "$(date '+%F %T') deleting $f (older NLC)"
    [ -z "$DRY" ] && rm -f -- "$f"
  done
fi
declare -A recent
while IFS= read -r -d '' f; do recent["$(key "$f")"]=1; done \
  < <(find . -maxdepth 1 -type f -mmin -$mins -printf '%P\0')
while IFS= read -r -d '' f; do
  if [ -n "${recent[$(key "$f")]}" ]; then
    echo "$(date '+%F %T') deleting $f"
    [ -z "$DRY" ] && rm -f -- "$f"
  fi
done < <(find . -maxdepth 1 -type f -mmin +$mins -printf '%P\0')

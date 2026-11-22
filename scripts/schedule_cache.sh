# Shared daily schedule cache/download flow for the scheduled maintenance scripts.
download_schedule_to() {
    local cache_file="$1"
    local destination="$2"
    local report_file="$3"
    local rdm_file="${SCHEDULE_SOURCE_PATH:-/srv/rdm/in/CIF_ALL_FULL_DAILY_toc-full.json.gz}"

    if [ -f "$cache_file" ] && [ "$(date -r "$cache_file" +%Y%m%d)" = "$(date +%Y%m%d)" ]; then
        cp "$cache_file" "$destination"
        echo "$(date '+%F %T') using app's same-day schedule cache" >> "$report_file"
    elif [ -f "$rdm_file" ]; then
        cp "$rdm_file" "$destination"
        echo "$(date '+%F %T') using RDM schedule (file dated $(date -r "$rdm_file" '+%F %T'))" >> "$report_file"
    else
        echo "$(date '+%F %T') no schedule available: cache stale and $rdm_file missing" >> "$report_file"
        return 1
    fi
}

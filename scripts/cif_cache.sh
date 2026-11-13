# Shared daily CIF cache/download flow for the scheduled CIF maintenance scripts.
download_cif_to() {
    local cache_file="$1"
    local destination="$2"
    local report_file="$3"
    local rdm_file="${CIF_SOURCE_PATH:-/srv/rdm/in/CIF_ALL_FULL_DAILY_toc-full.json.gz}"

    if [ -f "$cache_file" ] && [ "$(date -r "$cache_file" +%Y%m%d)" = "$(date +%Y%m%d)" ]; then
        cp "$cache_file" "$destination"
        echo "$(date '+%F %T') using app's same-day CIF cache" >> "$report_file"
    elif [ -f "$rdm_file" ]; then
        cp "$rdm_file" "$destination"
        echo "$(date '+%F %T') using RDM CIF (file dated $(date -r "$rdm_file" '+%F %T'))" >> "$report_file"
    else
        echo "$(date '+%F %T') no CIF available: cache stale and $rdm_file missing" >> "$report_file"
        return 1
    fi
}

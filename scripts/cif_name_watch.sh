#!/bin/bash
# Weekly watch: alert when TPS changes a name/CRS for any TIPLOC we have named.
# Compares today's CIF against the last snapshot; logs changes; updates snapshot.
set -u
SNAPDIR=~/traintracker-backend/scripts/cif_snapshots
mkdir -p "$SNAPDIR"
SNAP="$SNAPDIR/cif_snapshot_$(date +%Y%m%d).tsv"
WEEK_AGO="$SNAPDIR/cif_snapshot_$(date -d '7 days ago' +%Y%m%d).tsv"
[ -f "$WEEK_AGO" ] || WEEK_AGO=""
REPORT=~/traintracker-backend/logs/cif_name_changes.log
F=/tmp/cif_watch_daily.gz

CACHE=/opt/traintracker/cif_cache.gz
if [ -f "$CACHE" ] && [ "$(date -r "$CACHE" +%Y%m%d)" = "$(date +%Y%m%d)" ]; then
    cp "$CACHE" "$F"
    echo "$(date '+%F %T') using app's same-day CIF cache" >> "$REPORT"
else
    # env.conf is owned by opc - no sudo needed
    source <(grep -E '^SCHEDULE_(USERNAME|PASSWORD)' /opt/traintracker/env.conf)
    curl -sL -u "$SCHEDULE_USERNAME:$SCHEDULE_PASSWORD" -o "$F" \
      'https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full'
fi
[ "$(stat -c%s "$F")" -lt 1000000 ] && { echo "$(date '+%F %T') download failed" >> "$REPORT"; rm -f "$F"; exit 1; }

python3 - "$WEEK_AGO" "$SNAP" "$REPORT" "$F" << 'PY'
import gzip, json, sqlite3, sys, os, datetime
prev_path, snap_path, report_path, cif_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

con = sqlite3.connect("file:/opt/traintracker/traintracker.db?mode=ro", uri=True)
watched = {r[0] for r in con.execute("SELECT tiploc FROM location_overrides")}
con.close()

current = {}
with gzip.open(cif_path, "rt") as f:
    for line in f:
        if '"TiplocV1"' not in line: continue
        t = json.loads(line)["TiplocV1"]
        tip = t["tiploc_code"].strip()
        if tip in watched:
            current[tip] = ((t.get("tps_description") or "").strip(),
                            (t.get("crs_code") or "").strip())

old = {}
if prev_path and os.path.exists(prev_path):
    with open(prev_path) as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) == 3: old[p[0]] = (p[1], p[2])

changes = []
alert_rows = []
for tip, (desc, crs) in sorted(current.items()):
    if tip in old and old[tip] != (desc, crs):
        changes.append(f"{tip}: '{old[tip][0]}' [{old[tip][1]}] -> '{desc}' [{crs}]")
        alert_rows.append((tip, old[tip][0], old[tip][1], desc, crs))
for tip in sorted(set(old) & watched - set(current)):
    changes.append(f"{tip}: REMOVED from CIF (was '{old[tip][0]}')")
# Batch 60: the NEW branch is gone. Confirmed live 2026-07-21 that it reports
# false positives -- YORKTPE showed as "NEW in CIF" purely because its override
# was added THIS week, so it wasn't in `watched` (and therefore not in `old`)
# when last week's snapshot was taken, even though the TIPLOC has existed in
# CIF for years. "New" here means "newly watched by us", not "new to CIF" --
# not a signal worth alerting on.

stamp = datetime.datetime.now().strftime("%F %T")
with open(report_path, "a") as r:
    if changes:
        r.write(f"=== {stamp}: {len(changes)} change(s) ===\n")
        for c in changes: r.write(c + "\n")
    else:
        r.write(f"{stamp}: no changes ({len(current)} TIPLOCs checked)\n")

import json as _json
with open("/opt/traintracker/cif_changes.json", "w") as j:
    _json.dump({"last_run": stamp, "checked": len(current),
                "change_count": len(changes), "changes": changes}, j)

if alert_rows:
    con2 = sqlite3.connect("/opt/traintracker/traintracker.db")
    for tip, od, oc, nd, nc in alert_rows:
        con2.execute("""INSERT INTO cif_change_alerts (tiploc, old_desc, old_crs, new_desc, new_crs, first_seen, last_seen)
                         VALUES (?, ?, ?, ?, ?, ?, ?)
                         ON CONFLICT(tiploc, old_desc, old_crs, new_desc, new_crs)
                         DO UPDATE SET last_seen = excluded.last_seen""",
                      (tip, od, oc, nd, nc, stamp, stamp))
    con2.commit()
    con2.close()

with open(snap_path, "w") as s:
    for tip, (desc, crs) in sorted(current.items()):
        s.write(f"{tip}\t{desc}\t{crs}\n")
PY
find "$SNAPDIR" -name "cif_snapshot_*.tsv" -mtime +8 -delete
rm -f "$F"

# Prune log entries older than 90 days (blocks are grouped under a dated
# header/standalone line; any line lacking its own date inherits the most
# recent dated line's keep/drop decision, so multi-line change blocks stay intact).
python3 - "$REPORT" << 'PY'
import re, datetime, sys
path = sys.argv[1]
cutoff = datetime.date.today() - datetime.timedelta(days=90)
lines = open(path).read().splitlines(keepends=True)
out = []
keep = True
for line in lines:
    m = re.match(r'(\d{4}-\d{2}-\d{2})', line)
    if m:
        keep = datetime.date.fromisoformat(m.group(1)) >= cutoff
    if keep:
        out.append(line)
open(path, "w").writelines(out)
PY

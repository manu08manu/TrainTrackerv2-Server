
#!/bin/bash

set -u

DB=/opt/traintracker/traintracker.db
REPORT=/home/opc/traintracker-backend/logs/tiploc_override_sweep.log
F=/tmp/tiploc_sweep_daily.gz
CACHE="${CACHE_OVERRIDE:-/opt/traintracker/cif_cache.gz}"
GRACE_DAYS=30

mkdir -p "$(dirname "$REPORT")"

source "$(dirname "$0")/cif_cache.sh"
download_cif_to "$CACHE" "$F" "$REPORT"

if [ ! -f "$F" ] || [ "$(stat -c%s "$F")" -lt 1000000 ]; then
    echo "$(date '+%F %T') download failed / cache missing - sweep skipped" >> "$REPORT"
    rm -f "$F"
    exit 1
fi

python3 - "$DB" "$REPORT" "$F" "$GRACE_DAYS" << 'PY'
import gzip
import json
import sqlite3
import sys
import datetime

db_path, report_path, cif_path, grace_days = (
    sys.argv[1],
    sys.argv[2],
    sys.argv[3],
    int(sys.argv[4])
)

stamp = datetime.datetime.now().strftime("%F %T")
today = datetime.date.today()

current_cif = set()

with gzip.open(cif_path, "rt") as f:
    for line in f:
        if '"TiplocV1"' not in line:
            continue

        t = json.loads(line)["TiplocV1"]
        current_cif.add(t["tiploc_code"].strip())

con = sqlite3.connect(db_path)

con.execute("""
    CREATE TABLE IF NOT EXISTS tiploc_overrides_archive (
        tiploc TEXT PRIMARY KEY,
        name TEXT,
        crs TEXT,
        original_changed_by TEXT,
        original_changed_at TEXT,
        deleted_at TEXT
    )
""")

con.execute("""
    CREATE TABLE IF NOT EXISTS tiploc_missing_tracking (
        tiploc TEXT PRIMARY KEY,
        first_missing_at TEXT NOT NULL
    )
""")

restored, deleted, newly_missing, still_pending = [], [], [], []

# Restore: a previously-archived TIPLOC that has reappeared in CIF.
# Writes back into location_overrides rather than re-inserting,
# since nlc/stanme/stanox were never removed.

archived = con.execute("""
    SELECT tiploc, name, crs, original_changed_by, original_changed_at
    FROM tiploc_overrides_archive
""").fetchall()

for tiploc, name, crs, changed_by, changed_at in archived:
    if tiploc in current_cif:
        con.execute(
            """
            UPDATE location_overrides
            SET name = ?, crs = ?, changed_by = ?, changed_at = ?
            WHERE tiploc = ?
            """,
            (name, crs, "auto_restore", stamp, tiploc)
        )

        con.execute(
            "DELETE FROM tiploc_overrides_archive WHERE tiploc = ?",
            (tiploc,)
        )

        restored.append(tiploc)

tracked_map = dict(con.execute("""
    SELECT tiploc, first_missing_at
    FROM tiploc_missing_tracking
""").fetchall())

for tiploc in list(tracked_map):
    if tiploc in current_cif:
        con.execute(
            "DELETE FROM tiploc_missing_tracking WHERE tiploc = ?",
            (tiploc,)
        )
        del tracked_map[tiploc]

# Batch 2026-09-28: ported from the old tiploc name/CRS override tables
# to location_overrides as part of the
# location_overrides merge.
#
# A row counts as overridden if it has a non-blank name or CRS.
# A blank means not overridden on that field, matching
# CorpusLookup's loaders.

overrides = con.execute("""
    SELECT tiploc, name, crs, changed_by, changed_at
    FROM location_overrides
    WHERE LENGTH(name) != 0 OR LENGTH(crs) != 0
""").fetchall()

missing = [
    row for row in overrides
    if row[0] not in current_cif
]

for tiploc, name, crs, changed_by, changed_at in missing:
    if tiploc not in tracked_map:
        con.execute(
            """
            INSERT INTO tiploc_missing_tracking
                (tiploc, first_missing_at)
            VALUES (?, ?)
            """,
            (tiploc, stamp)
        )

        newly_missing.append(tiploc)

    else:
        first_missing = datetime.datetime.strptime(
            tracked_map[tiploc],
            "%Y-%m-%d %H:%M:%S"
        ).date()

        days_missing = (today - first_missing).days

        if days_missing >= grace_days:
            con.execute(
                """
                INSERT OR REPLACE INTO tiploc_overrides_archive
                    (tiploc, name, crs, original_changed_by,
                     original_changed_at, deleted_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (tiploc, name, crs, changed_by, changed_at, stamp)
            )

            # Blank name/CRS rather than deleting the row.
            # NLC/stanme/stanox stay, matching how a removed
            # override used to fall back to CIF.

            con.execute(
                """
                UPDATE location_overrides
                SET name = '', crs = ''
                WHERE tiploc = ?
                """,
                (tiploc,)
            )

            con.execute(
                "DELETE FROM tiploc_missing_tracking WHERE tiploc = ?",
                (tiploc,)
            )

            deleted.append(tiploc)

        else:
            still_pending.append(
                f"{tiploc} ({days_missing}/{grace_days}d)"
            )

con.commit()
con.close()

with open(report_path, "a") as r:
    if restored:
        r.write(
            f"{stamp}: restored {len(restored)} override(s): "
            f"{', '.join(restored)}\n"
        )

    if newly_missing:
        r.write(
            f"{stamp}: {len(newly_missing)} newly missing from CIF, "
            f"grace period started: {', '.join(newly_missing)}\n"
        )

    if deleted:
        r.write(
            f"{stamp}: archived {len(deleted)} override(s) after "
            f"{grace_days}-day grace period: {', '.join(deleted)}\n"
        )

    if still_pending:
        r.write(
            f"{stamp}: {len(still_pending)} still within grace period: "
            f"{', '.join(still_pending)}\n"
        )

    if not restored and not newly_missing and not deleted and not still_pending:
        r.write(
            f"{stamp}: no changes ({len(overrides)} overrides checked "
            f"against {len(current_cif)} CIF TIPLOCs)\n"
        )
PY

# Retention: prune sweep log entries older than 90 days,
# matching cif_name_watch.sh's established convention.
# This log was previously append-only forever.

python3 - "$REPORT" << 'PY2'
import sys
import datetime

path = sys.argv[1]

cutoff = datetime.date.today() - datetime.timedelta(days=90)

try:
    with open(path) as f:
        lines = f.readlines()
except FileNotFoundError:
    lines = []

kept = []

for line in lines:
    try:
        d = datetime.datetime.strptime(line[:10], "%Y-%m-%d").date()

        if d >= cutoff:
            kept.append(line)

    except ValueError:
        kept.append(line)  # Keep unparseable lines rather than silently drop them.

with open(path, "w") as f:
    f.writelines(kept)
PY2

rm -f "$F"

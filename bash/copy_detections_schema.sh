#!/bin/bash
#
# Copy the whole `detections` Postgres schema (DAG/ML tables: run, batch,
# zae_layer, inference, run_progression, ...) from a SOURCE db to a TARGET db.
# Intended direction: SOURCE = preprod, TARGET = prod.
#
# The source schema is first dumped to a local .sql backup in BACKUP_DIR
# (default /Volumes/data/aigle). The target schema is then DROPped and restored
# from that dump inside a single transaction: if anything fails, the target
# keeps its old schema untouched and the backup file remains for recovery.
#
# Config via env vars (see aigle-utils/.env.template). Load them with:
#   set -a && source .env && set +a
#
# Usage:
#   ./copy_detections_schema.sh           # asks for confirmation
#   ./copy_detections_schema.sh -y        # skip confirmation
#   ./copy_detections_schema.sh --check   # dry run: test both connections, print plan, do nothing

set -euo pipefail

SCHEMA=detections
BACKUP_DIR="${BACKUP_DIR:-/Volumes/data/aigle}"

ASSUME_YES=0
CHECK_ONLY=0
for arg in "$@"; do
    case "$arg" in
        -y|--yes)   ASSUME_YES=1 ;;
        --check)    CHECK_ONLY=1 ;;
        -h|--help)  sed -n '2,18p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

required=(
    SOURCE_DB_HOST SOURCE_DB_PORT SOURCE_DB_USER SOURCE_DB_PASSWORD SOURCE_DB_NAME
    TARGET_DB_HOST TARGET_DB_PORT TARGET_DB_USER TARGET_DB_PASSWORD TARGET_DB_NAME
)
for var in "${required[@]}"; do
    if [ -z "${!var:-}" ]; then
        echo "Missing required env var: $var (see aigle-utils/.env.template)" >&2
        exit 1
    fi
done

for bin in pg_dump psql; do
    command -v "$bin" >/dev/null || { echo "$bin not found in PATH" >&2; exit 1; }
done

# ponytail: SSL optional — managed target (Scaleway prod) usually needs sslmode=require, local doesn't.
source_psql() { PGPASSWORD="$SOURCE_DB_PASSWORD" PGSSLMODE="${SOURCE_DB_SSLMODE:-prefer}" psql -h "$SOURCE_DB_HOST" -p "$SOURCE_DB_PORT" -U "$SOURCE_DB_USER" -d "$SOURCE_DB_NAME" "$@"; }
target_psql() { PGPASSWORD="$TARGET_DB_PASSWORD" PGSSLMODE="${TARGET_DB_SSLMODE:-prefer}" psql -h "$TARGET_DB_HOST" -p "$TARGET_DB_PORT" -U "$TARGET_DB_USER" -d "$TARGET_DB_NAME" "$@"; }

echo "FROM (source): $SOURCE_DB_USER@$SOURCE_DB_HOST:$SOURCE_DB_PORT/$SOURCE_DB_NAME"
echo "  TO (target): $TARGET_DB_USER@$TARGET_DB_HOST:$TARGET_DB_PORT/$TARGET_DB_NAME"
echo "Schema to copy: $SCHEMA  (will be DROPPED then recreated on the TARGET)"

echo "Testing connections..."
source_psql -tAc 'SELECT 1' >/dev/null && echo "  source OK"
target_psql -tAc 'SELECT 1' >/dev/null && echo "  target OK"
source_psql -tAc "SELECT 1 FROM information_schema.schemata WHERE schema_name='$SCHEMA'" | grep -q 1 \
    || { echo "Schema '$SCHEMA' not found on source — nothing to copy." >&2; exit 1; }

if [ "$CHECK_ONLY" -eq 1 ]; then
    echo "--check: connections valid, source schema present. No changes made."
    exit 0
fi

if [ "$ASSUME_YES" -ne 1 ]; then
    printf "This will ERASE schema '%s' on the TARGET (%s) and replace it. Type 'yes' to continue: " "$SCHEMA" "$TARGET_DB_HOST/$TARGET_DB_NAME"
    read -r reply
    [ "$reply" = "yes" ] || { echo "Aborted."; exit 1; }
fi

mkdir -p "$BACKUP_DIR"
DUMP_FILE="$BACKUP_DIR/${SCHEMA}_$(date +%Y%m%d_%H%M%S).sql"
echo "Dumping '$SCHEMA' from source to $DUMP_FILE (safety backup)..."
PGPASSWORD="$SOURCE_DB_PASSWORD" PGSSLMODE="${SOURCE_DB_SSLMODE:-prefer}" \
    pg_dump -h "$SOURCE_DB_HOST" -p "$SOURCE_DB_PORT" -U "$SOURCE_DB_USER" -d "$SOURCE_DB_NAME" \
    --schema="$SCHEMA" --no-owner --no-privileges > "$DUMP_FILE"
echo "Backup saved: $DUMP_FILE ($(du -h "$DUMP_FILE" | cut -f1))"

# run.geozone_id is a FK into the public core_geozone.id, whose integer PKs DIFFER
# between environments — copied verbatim, target runs point at the wrong (or no)
# geozone, so the Imports view shows empty run names and finds no zae. We translate
# each source geozone_id to the target's id via a PORTABLE code (commune iso_code /
# department insee_code / epci siren_code / region insee_code). Custom/manual zones
# carry no such code and are left as-is (they show up in the report below).
# Only detections.run carries a geozone_id the app reads; plan uses a portable
# geozone_insee_code, and the stats_* tables are legacy analysis (not remapped).
echo "Building geozone id remap (source -> target, by portable code)..."
REMAP_INSERTS=$(source_psql -tAq -c "
    SELECT format('INSERT INTO _geozone_remap VALUES (%s, %L, %L);',
        gz.id, gz.geo_zone_type,
        COALESCE(gc.iso_code, gd.insee_code, ge.siren_code, gr.insee_code))
    FROM public.core_geozone gz
    LEFT JOIN public.core_geocommune    gc ON gc.geozone_ptr_id = gz.id
    LEFT JOIN public.core_geodepartment gd ON gd.geozone_ptr_id = gz.id
    LEFT JOIN public.core_geoepci       ge ON ge.geozone_ptr_id = gz.id
    LEFT JOIN public.core_georegion     gr ON gr.geozone_ptr_id = gz.id
    WHERE gz.id IN (SELECT DISTINCT geozone_id FROM $SCHEMA.run WHERE geozone_id IS NOT NULL)
")

echo "Restoring '$SCHEMA' into target + remapping geozone ids (single transaction)..."
# DROP + dump (CREATE SCHEMA included) + the geozone remap, all in ONE atomic transaction
# on the target. ON_ERROR_STOP + --single-transaction => all-or-nothing; on failure the
# old schema stays untouched. Public tables are schema-qualified because pg_dump resets
# search_path to ''. UPDATE...FROM evaluates the join against the pre-update snapshot, so
# source/target id overlaps don't cascade.
{
    echo "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"
    cat "$DUMP_FILE"
    echo "CREATE TEMP TABLE _geozone_remap (source_id bigint, gtype text, code text);"
    printf '%s\n' "$REMAP_INSERTS"
    cat <<SQL
UPDATE $SCHEMA.run r SET geozone_id = m.target_id
FROM (
    SELECT s.source_id, t.id AS target_id
    FROM _geozone_remap s
    JOIN public.core_geozone t ON t.geo_zone_type = s.gtype
    LEFT JOIN public.core_geocommune    tc ON tc.geozone_ptr_id = t.id
    LEFT JOIN public.core_geodepartment td ON td.geozone_ptr_id = t.id
    LEFT JOIN public.core_geoepci       te ON te.geozone_ptr_id = t.id
    LEFT JOIN public.core_georegion     tr ON tr.geozone_ptr_id = t.id
    WHERE s.code IS NOT NULL
      AND COALESCE(tc.iso_code, td.insee_code, te.siren_code, tr.insee_code) = s.code
) m
WHERE r.geozone_id = m.source_id AND r.geozone_id <> m.target_id;
SQL
} | target_psql --single-transaction -v ON_ERROR_STOP=1 -q

echo "Remap report (target):"
target_psql -tAc "
    SELECT format('  runs resolved to a target geozone: %s   unmapped (kept source id): %s',
        count(*) FILTER (WHERE gz.id IS NOT NULL),
        count(*) FILTER (WHERE r.geozone_id IS NOT NULL AND gz.id IS NULL))
    FROM $SCHEMA.run r LEFT JOIN public.core_geozone gz ON gz.id = r.geozone_id"

echo "Done. '$SCHEMA' copied + geozone ids remapped: source -> target. Backup kept at $DUMP_FILE."

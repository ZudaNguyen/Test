#!/usr/bin/env bash
set -euo pipefail

month="${1:?Expected YYYY-MM}"
trip_hash="${2:?Expected Parquet SHA-256}"
lookup_hash="${3:?Expected CSV SHA-256}"
[[ "$month" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]] || exit 2
[[ "$trip_hash" =~ ^[a-f0-9]{64}$ && "$lookup_hash" =~ ^[a-f0-9]{64}$ ]] || exit 2
hdfs=/opt/hadoop/bin/hdfs
raw=/opt/project/data/raw
year="${month:0:4}"
month_number="${month:5:2}"
trip_name="yellow_tripdata_${month}.parquet"

verify_local() {
  local actual
  actual=$(sha256sum "$1" | cut -d ' ' -f 1)
  [[ "$actual" == "$2" ]] || { echo "Local hash mismatch: $1" >&2; exit 1; }
}

put_verified() {
  local source="$1" destination="$2" expected="$3" actual
  "$hdfs" dfs -mkdir -p "$(dirname "$destination")"
  if "$hdfs" dfs -test -e "$destination"; then
    echo "EXISTS $destination; verifying content"
  else
    "$hdfs" dfs -put -f "$source" "${destination}.uploading"
    "$hdfs" dfs -mv "${destination}.uploading" "$destination"
  fi
  actual=$("$hdfs" dfs -cat "$destination" | sha256sum | cut -d ' ' -f 1)
  [[ "$actual" == "$expected" ]] || { echo "HDFS hash mismatch: $destination" >&2; exit 1; }
  echo "VERIFIED sha256=$actual path=$destination"
}

verify_local "$raw/$trip_name" "$trip_hash"
verify_local "$raw/taxi_zone_lookup.csv" "$lookup_hash"
timeout 180 "$hdfs" dfsadmin -safemode wait
put_verified "$raw/$trip_name" "/taxi/raw/yellow/year=$year/month=$month_number/$trip_name" "$trip_hash"
put_verified "$raw/taxi_zone_lookup.csv" /taxi/reference/taxi_zone_lookup.csv "$lookup_hash"
manifest="$raw/manifest-${month}.json"
manifest_hash=$(sha256sum "$manifest" | cut -d ' ' -f 1)
put_verified "$manifest" "/taxi/manifests/${month}.json" "$manifest_hash"
"$hdfs" dfs -ls -R /taxi

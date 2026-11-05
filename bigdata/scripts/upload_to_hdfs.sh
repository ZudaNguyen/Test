
#!/usr/bin/env bash

set -euo pipefail

# Usage:
# upload_to_hdfs.sh <local_file> <hdfs_directory>

LOCAL_FILE="${1:?Missing local file path}"
HDFS_DIR="${2:?Missing HDFS destination directory}"

FILENAME="$(basename "$LOCAL_FILE")"
HDFS_FILE="${HDFS_DIR%/}/${FILENAME}"

echo "======================================"
echo "        HDFS INGESTION"
echo "======================================"

# 1. Validate local file
if [[ ! -f "$LOCAL_FILE" ]]; then
    echo "ERROR: Source file does not exist: $LOCAL_FILE"
    exit 1
fi

LOCAL_SIZE=$(stat -c %s "$LOCAL_FILE")

echo "Source:      $LOCAL_FILE"
echo "Destination: $HDFS_FILE"
echo "Size:        $LOCAL_SIZE bytes"

# 2. Create HDFS directory if necessary
echo ""
echo "[1/3] Checking HDFS directory..."

hdfs dfs -mkdir -p "$HDFS_DIR"

# 3. Check whether the file already exists
echo ""
echo "[2/3] Checking existing HDFS file..."

if hdfs dfs -test -e "$HDFS_FILE"; then

    HDFS_SIZE=$(hdfs dfs -stat '%b' "$HDFS_FILE")

    if [[ "$LOCAL_SIZE" == "$HDFS_SIZE" ]]; then
        echo "SKIP: File already exists with matching size."
        exit 0
    else
        echo "ERROR: Existing file has a different size."
        echo "Local: $LOCAL_SIZE bytes"
        echo "HDFS:  $HDFS_SIZE bytes"
        echo "No overwrite performed."
        exit 1
    fi
fi

# 4. Upload file
echo ""
echo "[3/3] Uploading to HDFS..."

hdfs dfs -put "$LOCAL_FILE" "$HDFS_FILE"

# 5. Verify uploaded file
HDFS_SIZE=$(hdfs dfs -stat '%b' "$HDFS_FILE")

if [[ "$LOCAL_SIZE" != "$HDFS_SIZE" ]]; then
    echo "ERROR: Uploaded file size verification failed."
    exit 1
fi

echo ""
echo "SUCCESS: File uploaded and verified."
echo "HDFS path: $HDFS_FILE"
echo "Size:      $HDFS_SIZE bytes"

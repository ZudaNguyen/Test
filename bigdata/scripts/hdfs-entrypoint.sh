#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  namenode)
    mkdir -p /hadoop/dfs/name
    if [[ ! -f /hadoop/dfs/name/current/VERSION ]]; then
      if [[ -n "$(ls -A /hadoop/dfs/name)" ]]; then
        echo 'ERROR: NameNode directory is nonempty without VERSION; refusing to format.' >&2
        exit 1
      fi
      echo 'First start: formatting an empty NameNode volume.'
      /opt/hadoop/bin/hdfs namenode -format -nonInteractive -clusterid taxi-lab
    fi
    exec /opt/hadoop/bin/hdfs namenode
    ;;
  datanode)
    mkdir -p /hadoop/dfs/data
    exec /opt/hadoop/bin/hdfs datanode
    ;;
  *) echo 'Expected namenode or datanode.' >&2; exit 2 ;;
esac

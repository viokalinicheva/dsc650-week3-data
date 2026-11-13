#!/usr/bin/env bash
# I am starting the existing Thrift service only when its port is unused so I
# can connect HappyBase without restarting HBase or changing existing tables.
set -euo pipefail
cd /home/violettakalinicheva/dsc650-infra/bellevue-bigdata/hadoop-hive-spark-hbase
printf 'UTC start: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
docker compose exec -T master sh -eux -c '
  python3 -c "import socket; results=[]; hosts=(\"localhost\",\"master\");
for host in hosts:
 s=socket.socket(); s.settimeout(2); code=s.connect_ex((host,9090)); s.close(); results.append(code); print(host,9090,\"connect_ex\",code)
assert all(code != 0 for code in results), \"A Thrift listener already exists; do not start a duplicate\""
  "$HBASE_HOME/bin/hbase-daemon.sh" start thrift
  python3 -c "import time,happybase;
for attempt in range(1,31):
 try:
  connection=happybase.Connection(\"master\",port=9090,timeout=3000); print(\"Thrift ready; existing tables\",connection.tables()); connection.close(); break
 except Exception as error:
  print(\"Readiness attempt\",attempt,type(error).__name__,str(error));
  if attempt == 30: raise
  time.sleep(2)"
  jps
' </dev/null
printf 'UTC finish: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

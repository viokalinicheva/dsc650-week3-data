#!/usr/bin/env bash
# I am installing only the course project's pinned dependencies so I can keep
# the existing Python 3.7 and Spark 3.0 environment unchanged.
set -euo pipefail

stack_directory="/home/violettakalinicheva/dsc650-infra/bellevue-bigdata/hadoop-hive-spark-hbase"
node_name="${1:-master}"
case "$node_name" in
  master|worker1|worker2) ;;
  *) printf 'Usage: bash install_dependencies.sh {master|worker1|worker2}\n' >&2; exit 2 ;;
esac

printf 'UTC start: %s\nNode: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$node_name"
cd "$stack_directory"
docker compose exec -T "$node_name" sh -eux -c '
  python3 --version
  python3 -m pip --version
  apk --version
  apk add --no-cache --repositories-file /dev/null \
    --repository https://dl-cdn.alpinelinux.org/alpine/v3.10/main \
    --repository https://dl-cdn.alpinelinux.org/alpine/v3.10/community \
    "py3-numpy=1.16.4-r1" "cython3=0.29.10-r0"
  python3 -m pip install --disable-pip-version-check --no-cache-dir \
    --index-url https://pypi.org/simple --no-deps \
    "wheel==0.41.3" "six==1.16.0" "ply==3.11"
  python3 -m pip install --disable-pip-version-check --no-cache-dir \
    --index-url https://pypi.org/simple --no-deps --no-build-isolation \
    "thriftpy2==0.4.20" "happybase==1.2.0"
  python3 -c "import sys,numpy,Cython,happybase,thriftpy2,six,ply,setuptools; print(\"Python\",sys.version); print(\"numpy\",numpy.__version__); print(\"Cython\",Cython.__version__); print(\"happybase\",happybase.__version__); print(\"thriftpy2\",thriftpy2.__version__); print(\"six\",six.__version__); print(\"ply\",ply.__version__); print(\"setuptools\",setuptools.__version__)"
  python3 -m pip check
  apk info -v py3-numpy cython3
  python3 -m pip freeze --all
' </dev/null
printf 'UTC finish: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

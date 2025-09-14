#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
export PYTHONUNBUFFERED=1

cd /root/Echelon/valentrix

exec /root/Echelon/bin/python3 -u -m mm.utils.partition_manager.edith \
  >> mm/utils/partition_manager/partition_manager.log 2>&1

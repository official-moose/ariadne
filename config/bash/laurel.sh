#!/usr/bin/env bash 
set -Eeuo pipefail 
umask 077 
export PYTHONUNBUFFERED=1 

cd /root/Echelon/valentrix 

exec /root/Echelon/bin/python3 -u -m mm.utils.canary.laurel \ 
 >> mm/utils/canary/laurel.log 2>&1

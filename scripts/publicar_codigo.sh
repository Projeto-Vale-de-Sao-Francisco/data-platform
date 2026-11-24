#!/usr/bin/env bash
# Envia src/ para /Volumes/campo_vivo/bronze/code e seeds/ para o landing de cadastros.
# Requer DATABRICKS_HOST e DATABRICKS_TOKEN (escopo files) no ambiente.
set -euo pipefail
cd "$(dirname "$0")/.."
put() {
  code=$(curl -s -m 300 -X PUT -H "Authorization: Bearer ${DATABRICKS_TOKEN}" --data-binary @"$1" \
    "${DATABRICKS_HOST%/}/api/2.0/fs/files$2?overwrite=true" -o /dev/null -w "%{http_code}")
  echo "$code $2"; [ "$code" = "204" ] || exit 1
}
for f in $(cd src && find . -name '*.py'); do put "src/$f" "/Volumes/campo_vivo/bronze/code/${f#./}"; done
for f in seeds/*.json; do put "$f" "/Volumes/campo_vivo/bronze/landing/cadastros/$(basename "$f")"; done

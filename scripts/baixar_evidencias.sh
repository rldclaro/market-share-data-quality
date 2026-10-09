#!/usr/bin/env bash
# Copia as evidências geradas pela task `evidencias` (Volume ms_dq.evidencias) para docs/evidencias/.
# Uso: ./scripts/baixar_evidencias.sh [perfil] [catalogo]
set -euo pipefail

PROFILE="${1:-gf7}"
CATALOG="${2:-workspace}"
ORIGEM="dbfs:/Volumes/${CATALOG}/ms_dq/evidencias"
DESTINO="docs/evidencias"

mkdir -p "${DESTINO}"
echo "Copiando ${ORIGEM} -> ${DESTINO}"
databricks fs cp -r --overwrite "${ORIGEM}" "${DESTINO}" --profile "${PROFILE}"
echo "Pronto. Confira com: git status ${DESTINO}"

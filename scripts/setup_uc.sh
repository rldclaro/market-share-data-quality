#!/usr/bin/env bash
# Prepara o Unity Catalog e envia os dados de entrada para o Volume de landing.
# Uso: ./scripts/setup_uc.sh [perfil] [catalogo]
set -euo pipefail

PROFILE="${1:-gf7}"
CATALOG="${2:-workspace}"
LANDING="dbfs:/Volumes/${CATALOG}/ms_raw/landing"

echo ">> Schemas"
for s in ms_raw ms_bronze ms_silver ms_gold ms_dq; do
  if databricks schemas get "${CATALOG}.${s}" --profile "$PROFILE" >/dev/null 2>&1; then
    echo "   ${CATALOG}.${s} já existe"
  else
    databricks schemas create "$s" "$CATALOG" --profile "$PROFILE" >/dev/null
    echo "   ${CATALOG}.${s} criado"
  fi
done

echo ">> Volume"
if databricks volumes read "${CATALOG}.ms_raw.landing" --profile "$PROFILE" >/dev/null 2>&1; then
  echo "   ${CATALOG}.ms_raw.landing já existe"
else
  databricks volumes create "$CATALOG" ms_raw landing MANAGED --profile "$PROFILE" >/dev/null
  echo "   ${CATALOG}.ms_raw.landing criado"
fi

echo ">> Upload (dados pessoais não sobem: minimização LGPD)"
for f in data/raw/*.csv data/raw/README.md; do
  case "$(basename "$f")" in
    sensitive_*) echo "   ignorado: $f"; continue ;;
  esac
  databricks fs cp "$f" "${LANDING}/" --overwrite --profile "$PROFILE" >/dev/null
  echo "   enviado: $(basename "$f")"
done

echo ">> Conteúdo do Volume"
databricks fs ls "${LANDING}/" --profile "$PROFILE"
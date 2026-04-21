#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Shared configuration for HeliosCTA Azure infrastructure scripts.
# Source this file; do not execute directly.
# ------------------------------------------------------------------------------

# Subscription and region
SUBSCRIPTION="ec7843fc-6505-4677-b84f-f4c613b2914d"
LOCATION="eastus2"

# Resource Group
RG="helioscta-backend-rg"

# Azure Managed Grafana
GRAFANA_NAME="amg-helioscta-backend"
GRAFANA_SKU_TIER="Standard"
GRAFANA_VIEWERS_GROUP_NAME="grafana-viewers"
GRAFANA_VIEWERS_GROUP_DESCRIPTION="Org-wide read-only access to Azure Managed Grafana dashboards."

# Azure Storage Account (Blob)
STORAGE_ACCOUNT="sthelioscta"
STORAGE_CONTAINER="helioscta"
STORAGE_SKU="Standard_LRS"

# PJM Modelling Database (separate resource group)
PJM_RG="helioscta-pjm-rg"
PJM_PG_SERVER="psql-helioscta-pjm"
PJM_PG_ADMIN="pjmadmin"
PJM_PG_PASSWORD="Pjmadmin1!"
PJM_PG_DB="pjm"

# Optional local-only secrets/overrides file (ignored by git).
# Create azure-infra/config.secrets.sh on your machine for passwords.
CONFIG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_SECRETS_FILE="$CONFIG_DIR/config.secrets.sh"
if [[ -f "$LOCAL_SECRETS_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$LOCAL_SECRETS_FILE"
fi
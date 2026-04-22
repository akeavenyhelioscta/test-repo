#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Phase 1: Provision PJM modelling Postgres database in Azure.
#
# This script provisions:
#   1) Resource Group
#   2) Azure PostgreSQL Flexible Server + database
#
# Prerequisites:
#   - Azure CLI installed and authenticated (az login)
#   - config.sh populated (subscription, location, DB names)
#
# Usage:
#   bash azure-infra/01-provision-infrastructure.sh
# ------------------------------------------------------------------------------

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

# Helper: run az with MSYS_NO_PATHCONV to prevent Git Bash mangling /subscriptions/... paths
az() { MSYS_NO_PATHCONV=1 command az "$@"; }

if [[ "$SUBSCRIPTION" == "<your-subscription-id>" ]]; then
  echo "ERROR: Set SUBSCRIPTION in config.sh before running." >&2
  exit 1
fi

echo ""
echo "=== Setting subscription ==="
az account set --subscription "$SUBSCRIPTION"

echo ""
if az group show --name "$RG" --output none 2>/dev/null; then
  echo "=== Resource Group '$RG' already exists - skipping ==="
else
  echo "=== Creating Resource Group: $RG ==="
  az group create \
    --name "$RG" \
    --location "$LOCATION" \
    --output none
fi

echo ""
if az postgres flexible-server show --resource-group "$RG" --name "$PG_SERVER" --output none 2>/dev/null; then
  echo "=== PostgreSQL Server '$PG_SERVER' already exists - skipping ==="
else
  echo "=== Creating PostgreSQL Server: $PG_SERVER ==="
  az postgres flexible-server create \
    --resource-group "$RG" \
    --name "$PG_SERVER" \
    --location "$LOCATION" \
    --admin-user "$PG_ADMIN" \
    --admin-password "$PG_PASSWORD" \
    --sku-name Standard_B1ms \
    --tier Burstable \
    --storage-size 32 \
    --version 15 \
    --yes \
    --output none
fi

echo "=== Ensuring Database exists: $PG_DB ==="
az postgres flexible-server db create \
  --resource-group "$RG" \
  --server-name "$PG_SERVER" \
  --database-name "$PG_DB" \
  --output none 2>/dev/null || true

echo "=== Ensuring firewall rule exists: AllowAzureServices ==="
az postgres flexible-server firewall-rule create \
  --resource-group "$RG" \
  --name "$PG_SERVER" \
  --rule-name "AllowAzureServices" \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0 \
  --output none 2>/dev/null || true

PG_HOST="${PG_SERVER}.postgres.database.azure.com"

echo ""
echo "=== Infrastructure provisioning complete ==="
echo ""
echo "Resources ready:"
echo "  Resource Group:      $RG"
echo "  PostgreSQL Server:   $PG_SERVER"
echo "  PostgreSQL Database: $PG_DB"
echo "  Host:                $PG_HOST"

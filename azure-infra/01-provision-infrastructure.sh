#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Phase 1: Provision minimal Azure infrastructure for local/backend testing.
#
# This script provisions:
#   1) Backend Resource Group
#   2) Azure Managed Grafana
#   3) Entra group for org-wide Grafana dashboard viewing
#   4) PJM modelling Resource Group + Azure PostgreSQL Flexible Server + database
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

ensure_amg_extension() {
  if az extension show --name amg --output none 2>/dev/null; then
    return
  fi

  echo "=== Installing Azure CLI extension: amg ==="
  az extension add --name amg --allow-preview true --yes --output none
}

ensure_grafana_viewers_group_assignment() {
  if [[ -z "${GRAFANA_VIEWERS_GROUP_NAME:-}" ]]; then
    echo "=== GRAFANA_VIEWERS_GROUP_NAME is empty - skipping Grafana viewer group setup ==="
    return
  fi

  local group_id
  local mail_nickname
  local grafana_id
  local assignment_count

  group_id=$(az ad group list \
    --filter "displayName eq '$GRAFANA_VIEWERS_GROUP_NAME'" \
    --query "[0].id" \
    -o tsv 2>/dev/null || true)

  if [[ -z "$group_id" ]]; then
    mail_nickname=$(echo "$GRAFANA_VIEWERS_GROUP_NAME" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9')
    if [[ -z "$mail_nickname" ]]; then
      mail_nickname="grafanaviewers"
    fi

    echo "=== Creating Entra group: $GRAFANA_VIEWERS_GROUP_NAME ==="
    az ad group create \
      --display-name "$GRAFANA_VIEWERS_GROUP_NAME" \
      --mail-nickname "$mail_nickname" \
      --description "${GRAFANA_VIEWERS_GROUP_DESCRIPTION:-Org-wide read-only access to Azure Managed Grafana dashboards.}" \
      --output none

    group_id=$(az ad group list \
      --filter "displayName eq '$GRAFANA_VIEWERS_GROUP_NAME'" \
      --query "[0].id" \
      -o tsv)
  else
    echo "=== Entra group '$GRAFANA_VIEWERS_GROUP_NAME' already exists - skipping create ==="
  fi

  grafana_id=$(az grafana show \
    --resource-group "$RG" \
    --name "$GRAFANA_NAME" \
    --query id \
    -o tsv)

  assignment_count=$(az role assignment list \
    --assignee-object-id "$group_id" \
    --scope "$grafana_id" \
    --query "[?roleDefinitionName=='Grafana Viewer'] | length(@)" \
    -o tsv 2>/dev/null || echo "0")

  if [[ "$assignment_count" == "0" ]]; then
    echo "=== Assigning 'Grafana Viewer' to group '$GRAFANA_VIEWERS_GROUP_NAME' on '$GRAFANA_NAME' ==="
    local max_attempts=6
    local attempt=1
    while (( attempt <= max_attempts )); do
      if az role assignment create \
        --assignee-object-id "$group_id" \
        --assignee-principal-type Group \
        --role "Grafana Viewer" \
        --scope "$grafana_id" \
        --output none 2>/dev/null; then
        break
      fi

      if (( attempt == max_attempts )); then
        echo "ERROR: Failed to assign Grafana Viewer to '$GRAFANA_VIEWERS_GROUP_NAME' after $max_attempts attempts." >&2
        return 1
      fi

      echo "    Role assignment not ready yet (Entra replication). Retrying in 10s..."
      sleep 10
      ((attempt++))
    done
  else
    echo "=== Role assignment already exists: '$GRAFANA_VIEWERS_GROUP_NAME' has Grafana Viewer on '$GRAFANA_NAME' ==="
  fi
}

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
ensure_amg_extension
if az grafana show --resource-group "$RG" --name "$GRAFANA_NAME" --output none 2>/dev/null; then
  echo "=== Azure Managed Grafana '$GRAFANA_NAME' already exists - skipping ==="
else
  echo "=== Creating Azure Managed Grafana: $GRAFANA_NAME ==="
  az grafana create \
    --resource-group "$RG" \
    --name "$GRAFANA_NAME" \
    --location "$LOCATION" \
    --sku-tier "$GRAFANA_SKU_TIER" \
    --output none
fi

echo ""
ensure_grafana_viewers_group_assignment

echo ""
if az group show --name "$PJM_RG" --output none 2>/dev/null; then
  echo "=== PJM Resource Group '$PJM_RG' already exists - skipping ==="
else
  echo "=== Creating PJM Resource Group: $PJM_RG ==="
  az group create \
    --name "$PJM_RG" \
    --location "$LOCATION" \
    --output none
fi

echo ""
if az postgres flexible-server show --resource-group "$PJM_RG" --name "$PJM_PG_SERVER" --output none 2>/dev/null; then
  echo "=== PJM PostgreSQL Server '$PJM_PG_SERVER' already exists - skipping ==="
else
  echo "=== Creating PJM PostgreSQL Server: $PJM_PG_SERVER ==="
  az postgres flexible-server create \
    --resource-group "$PJM_RG" \
    --name "$PJM_PG_SERVER" \
    --location "$LOCATION" \
    --admin-user "$PJM_PG_ADMIN" \
    --admin-password "$PJM_PG_PASSWORD" \
    --sku-name Standard_B1ms \
    --tier Burstable \
    --storage-size 32 \
    --version 15 \
    --yes \
    --output none
fi

echo "=== Ensuring PJM Database exists: $PJM_PG_DB ==="
az postgres flexible-server db create \
  --resource-group "$PJM_RG" \
  --server-name "$PJM_PG_SERVER" \
  --database-name "$PJM_PG_DB" \
  --output none 2>/dev/null || true

echo "=== Ensuring firewall rule exists: AllowAzureServices ==="
az postgres flexible-server firewall-rule create \
  --resource-group "$PJM_RG" \
  --name "$PJM_PG_SERVER" \
  --rule-name "AllowAzureServices" \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0 \
  --output none 2>/dev/null || true

PJM_HOST="${PJM_PG_SERVER}.postgres.database.azure.com"
GRAFANA_ENDPOINT=$(az grafana show \
  --resource-group "$RG" \
  --name "$GRAFANA_NAME" \
  --query "properties.endpoint" \
  -o tsv 2>/dev/null || true)

echo ""
echo "=== Infrastructure provisioning complete ==="
echo ""
echo "Resources ready:"
echo "  Backend Resource Group: $RG"
echo "  Azure Managed Grafana:  $GRAFANA_NAME"
echo "  Grafana Viewer Group:   ${GRAFANA_VIEWERS_GROUP_NAME:-<not-set>}"
echo "  Grafana Endpoint:       ${GRAFANA_ENDPOINT:-<not-available-yet>}"
echo "  PJM Resource Group:     $PJM_RG"
echo "  PJM PostgreSQL Server:  $PJM_PG_SERVER"
echo "  PJM PostgreSQL DB:      $PJM_PG_DB"
echo "  PJM Host:               $PJM_HOST"
echo ""
echo "Next step: bash azure-infra/02-provision-action-group-push.sh"

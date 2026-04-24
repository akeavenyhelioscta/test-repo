#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Phase 1: Provision PJM modelling blob storage in Azure.
#
# This script provisions:
#   1) Resource Group
#   2) Azure Storage Account + Blob Container (model cache)
#
# Prerequisites:
#   - Azure CLI installed and authenticated (az login)
#   - config.sh populated (subscription, location, storage names)
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
if az storage account show --resource-group "$RG" --name "$STORAGE_ACCOUNT" --output none 2>/dev/null; then
  echo "=== Storage Account '$STORAGE_ACCOUNT' already exists - skipping ==="
else
  echo "=== Creating Storage Account: $STORAGE_ACCOUNT ==="
  az storage account create \
    --resource-group "$RG" \
    --name "$STORAGE_ACCOUNT" \
    --location "$LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --allow-blob-public-access false \
    --min-tls-version TLS1_2 \
    --output none
fi

echo "=== Fetching storage account key ==="
STORAGE_KEY=$(az storage account keys list \
  --resource-group "$RG" \
  --account-name "$STORAGE_ACCOUNT" \
  --query "[0].value" -o tsv)

echo "=== Ensuring Blob Container exists: $STORAGE_CONTAINER ==="
az storage container create \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$STORAGE_KEY" \
  --name "$STORAGE_CONTAINER" \
  --output none 2>/dev/null || true

echo ""
echo "=== Infrastructure provisioning complete ==="
echo ""
echo "Resources ready:"
echo "  Resource Group:   $RG"
echo "  Storage Account:  $STORAGE_ACCOUNT"
echo "  Blob Container:   $STORAGE_CONTAINER"
echo ""
echo "Fetch the connection string when you need it (do not commit):"
echo "  az storage account show-connection-string \\"
echo "    --resource-group $RG --name $STORAGE_ACCOUNT --query connectionString -o tsv"
echo ""
echo "Then set on the Prefect worker:"
echo "  MODEL_CACHE_BLOB_ENABLED=true"
echo "  MODEL_CACHE_BLOB_CONTAINER=$STORAGE_CONTAINER"
echo "  AZURE_STORAGE_CONNECTION_STRING='<paste connection string>'"

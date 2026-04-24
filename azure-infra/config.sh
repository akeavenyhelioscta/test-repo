#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Shared configuration for HeliosCTA PJM Azure infrastructure scripts.
# Source this file; do not execute directly.
# ------------------------------------------------------------------------------

# Subscription and region
SUBSCRIPTION="ec7843fc-6505-4677-b84f-f4c613b2914d"
LOCATION="eastus2"

# Resource Group
RG="helioscta-pjm-rg"

# Blob Storage (model cache)
# Storage account name must be globally unique, 3-24 chars, lowercase alphanumeric.
STORAGE_ACCOUNT="stheliosctapjm"
STORAGE_CONTAINER="model-cache"

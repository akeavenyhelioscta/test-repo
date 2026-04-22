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

# PostgreSQL
PG_SERVER="psql-helioscta-pjm"
PG_ADMIN="pjmadmin"
PG_PASSWORD="Pjmadmin1!"
PG_DB="pjm"

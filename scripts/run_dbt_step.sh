#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../terraform"

CURRENT_IP=$(curl -4 -s ifconfig.me)
echo "Current public IP: $CURRENT_IP"

echo "==> Opening Redshift to public access for dbt..."
terraform apply -var="redshift_publicly_accessible=true" \
  -var="dbt_local_access_cidr=${CURRENT_IP}/32" \
  -var="redshift_admin_password=$TF_VAR_redshift_admin_password" -auto-approve

cd ../dbt_project
echo "==> Running dbt seed/run/test..."
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .

cd ../terraform
echo "==> Closing Redshift back to private-only for QuickSight..."
terraform apply -var="redshift_admin_password=$TF_VAR_redshift_admin_password" -auto-approve

echo "Done. Redshift is private again. dbt succeeded — proceed to QuickSight verification."
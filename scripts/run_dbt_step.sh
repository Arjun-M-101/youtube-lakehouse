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

# Docs need a live warehouse connection to build the catalog, so generate them
# while the workgroup is still open. --static writes one self-contained page:
# dbt_project/target/static_index.html (open it in a browser; no server needed).
# A docs failure must not leave the workgroup open, so it is non-fatal.
echo "==> Generating dbt docs (lineage graph)..."
dbt docs generate --static --profiles-dir . \
  || echo "WARNING: dbt docs generate failed - tests passed, continuing to close Redshift."

cd ../terraform
echo "==> Closing Redshift back to private-only for QuickSight..."
terraform apply -var="redshift_admin_password=$TF_VAR_redshift_admin_password" -auto-approve

echo "Done. Redshift is private again. dbt succeeded - proceed to QuickSight verification."
echo "dbt docs: open dbt_project/target/static_index.html in a browser (lineage icon, bottom-right)."
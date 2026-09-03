#!/usr/bin/env bash
set -euo pipefail

PREFIX="youtube-lakehouse"

echo "Discovering buckets matching '${PREFIX}*'..."
BUCKETS=$(aws s3api list-buckets --query "Buckets[?starts_with(Name, '${PREFIX}')].Name" --output text)

if [ -z "$BUCKETS" ]; then
  echo "No buckets found matching '${PREFIX}*'. Nothing to clean."
  exit 0
fi

echo "Found bucket(s):"
echo "$BUCKETS" | tr '\t' '\n'
echo

for BUCKET in $BUCKETS; do
  echo "=== Cleaning $BUCKET ==="

  python3 - <<PY
import boto3

bucket = "$BUCKET"
s3 = boto3.client("s3")
paginator = s3.get_paginator("list_object_versions")

total_versions, total_markers = 0, 0
for page in paginator.paginate(Bucket=bucket):
    objects = []
    for v in page.get("Versions", []):
        objects.append({"Key": v["Key"], "VersionId": v["VersionId"]})
        total_versions += 1
    for m in page.get("DeleteMarkers", []):
        objects.append({"Key": m["Key"], "VersionId": m["VersionId"]})
        total_markers += 1
    if objects:
        for i in range(0, len(objects), 1000):
            batch = objects[i:i + 1000]
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})

print(f"  Deleted versions: {total_versions}")
print(f"  Deleted delete markers: {total_markers}")
PY

  echo "  Verify: aws s3api list-object-versions --bucket $BUCKET --output json --no-cli-pager"
  echo
done

echo "Done. All matching buckets cleaned — retry terraform destroy now."
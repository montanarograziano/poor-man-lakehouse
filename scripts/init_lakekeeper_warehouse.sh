#!/bin/sh

# Initialize Lakekeeper warehouse with environment variables
# This script avoids hardcoding credentials in JSON files

set -e

# Set default values if not provided
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-"minioadmin"}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-"miniopassword"}
AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-"http://minio:9000"}
AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-"eu-central-1"}
BUCKET_NAME=${BUCKET_NAME:-"warehouse"}
LAKEKEEPER_SERVER_URI=${LAKEKEEPER_SERVER_URI:-"http://lakekeeper:8181"}

echo "Initializing Lakekeeper warehouse..."
echo "  Endpoint: ${LAKEKEEPER_SERVER_URI}"
echo "  Bucket: ${BUCKET_NAME}"
echo "  S3 Endpoint: ${AWS_ENDPOINT_URL}"

# Construct the JSON payload with environment variables
PAYLOAD=$(cat <<EOF
{
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-credential": {
    "aws-access-key-id": "${AWS_ACCESS_KEY_ID}",
    "aws-secret-access-key": "${AWS_SECRET_ACCESS_KEY}",
    "credential-type": "access-key",
    "type": "s3"
  },
  "storage-profile": {
    "assume-role-arn": null,
    "bucket": "${BUCKET_NAME}",
    "endpoint": "${AWS_ENDPOINT_URL}",
    "flavor": "minio",
    "key-prefix": "lakekeeper",
    "path-style-access": true,
    "region": "${AWS_DEFAULT_REGION}",
    "sts-enabled": true,
    "type": "s3"
  },
  "warehouse-name": "warehouse"
}
EOF
)

echo "Sending warehouse creation request..."

# Send the request
RESPONSE=$(curl -s -w "\n%{http_code}" \
  -X POST \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}" \
  "${LAKEKEEPER_SERVER_URI}/management/v1/warehouse")

# Extract status code (last line) and body (everything else)
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
    echo "Warehouse created successfully (HTTP ${HTTP_CODE})"
    echo "Response: ${BODY}"
elif [ "$HTTP_CODE" = "409" ]; then
    echo "Warehouse already exists (HTTP ${HTTP_CODE})"
else
    echo "Failed to create warehouse (HTTP ${HTTP_CODE})"
    echo "Response: ${BODY}"
    exit 1
fi

#!/bin/sh

# Load environment variables (if .env file exists)
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Set default values if not provided
DREMIO_USERNAME=${DREMIO_USERNAME:-"dremio"}
DREMIO_ROOT_PASSWORD=${DREMIO_ROOT_PASSWORD:-"dremio123"}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-"minioadmin"}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-"miniopassword"}
AWS_ENDPOINT=${AWS_ENDPOINT:-"http://minio:9000"}
BUCKET_NAME=${BUCKET_NAME:-"warehouse"}
NESSIE_DREMIO_SERVER_URI=${NESSIE_DREMIO_SERVER_URI:-"http://nessie:19120/api/v2"}

echo "Authenticating with Dremio..."

# Function to get token from Dremio (replicating Python _get_token() function)
_get_token() {
    local login_endpoint="http://dremio:9047/apiv2/login"
    local payload="{\"userName\": \"${DREMIO_USERNAME}\", \"password\": \"${DREMIO_ROOT_PASSWORD}\"}"

    # Make a single POST request and capture both response and status code
    local temp_file=$(mktemp)
    local status_code=$(curl -s -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "${payload}" "${login_endpoint}" -o "${temp_file}")
    local response=$(cat "${temp_file}")
    rm -f "${temp_file}"

    # Check if the request was successful
    if [ "${status_code}" = "200" ]; then
        # Parse the JSON response to extract the token
        local token=$(echo "${response}" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)

        if [ -z "${token}" ] || [ "${token}" = "" ]; then
            echo "Failed to retrieve a valid token from Dremio." >&2
            return 1
        fi

        echo "${token}"
        return 0
    else
        echo "Failed to get a valid response. Status code: ${status_code}" >&2
        echo "Response: ${response}" >&2
        return 1
    fi
}

# Authenticate with Dremio and get a token
echo "Attempting to authenticate with user: ${DREMIO_USERNAME} ${DREMIO_ROOT_PASSWORD}"
TOKEN=$(_get_token)

# Check for successful authentication
if [ -z "$TOKEN" ]; then
    echo "Failed to get Dremio authentication token. Exiting."
    exit 1
fi

echo "Successfully authenticated. Token length: ${#TOKEN}"

# Check if Nessie catalog already exists
curl -s -X GET -H "Authorization: Bearer ${TOKEN}" http://dremio:9047/api/v3/catalog/nessie | grep -q '"name":"nessie"'
if [ $? -eq 0 ]; then
    echo "Nessie catalog 'nessie' already exists. Skipping creation."
else
    echo "Nessie catalog 'nessie' not found. Creating..."

    # Create a temporary file with the JSON payload to avoid shell escaping issues
    TEMP_PAYLOAD=$(mktemp)
    cat > "$TEMP_PAYLOAD" << 'EOF'
{
    "name": "nessie",
    "config": {
        "nessieEndpoint": "http://nessie:19120/api/v2",
        "nessieAuthType": "NONE",
        "storageProvider": "AWS",
        "awsRootPath": "warehouse",
        "credentialType": "ACCESS_KEY",
        "awsAccessKey": "minioadmin",
        "awsAccessSecret": "miniopassword",
        "azureAuthenticationType": "ACCESS_KEY",
        "googleAuthenticationType": "SERVICE_ACCOUNT_KEYS",
        "propertyList": [
            {
                "name": "fs.s3a.path.style.access",
                "value": "true"
            },
            {
                "name": "fs.s3a.endpoint",
                "value": "minio:9000"
            },
            {
                "name": "dremio.s3.compat",
                "value": "true"
            }
        ],
        "secure": false,
        "asyncEnabled": true,
        "isCachingEnabled": true,
        "maxCacheSpacePct": 100
    },
    "accelerationRefreshPeriod": 3600000,
    "accelerationGracePeriod": 10800000,
    "accelerationActivePolicyType": "PERIOD",
    "accelerationRefreshSchedule": "0 0 8 * * *",
    "accelerationRefreshOnDataChanges": false,
    "type": "NESSIE",
    "accessControlList": {
        "userControls": [],
        "roleControls": []
    }
}
EOF

    echo "Payload to send:"
    cat "$TEMP_PAYLOAD"

    # Send the request to create the source
    RESPONSE=$(curl -s -X PUT \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      -d "@$TEMP_PAYLOAD" \
      http://dremio:9047/apiv2/source/nessie)

    echo "Response from Dremio:"
    echo "$RESPONSE"

    # Clean up the temporary file
    rm -f "$TEMP_PAYLOAD"
fi
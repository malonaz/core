#!/usr/bin/env bash
set -e


eval "$(ansible-vault view ~/toolchain/deploy/tsunade/secrets.yaml --vault-password-file /secrets/malonaz/ansible-vault-password | \
  python3 -c "
import sys, yaml, json
data = yaml.safe_load(sys.stdin)
mappings = {
    'AI_SERVICE_ANTHROPIC_API_KEY': 'anthropic_api_key',
    'AI_SERVICE_GROQ_API_KEY': 'groq_api_key',
    'AI_SERVICE_XAI_API_KEY': 'xai_api_key',
    'AI_SERVICE_OPENAI_API_KEY': 'openai_api_key',
    'AI_SERVICE_CEREBRAS_API_KEY': 'cerebras_api_key',
    'AI_SERVICE_DEEPGRAM_API_KEY': 'deepgram_api_key',
    'AI_SERVICE_GOOGLE_CLOUD_PROJECT': 'google_cloud_project',
    'AI_SERVICE_GOOGLE_CLOUD_LOCATION': 'google_cloud_location',
}
for env_var, yaml_key in mappings.items():
    print(f'export {env_var}={data.get(yaml_key, \"\")}')
val = data.get('google_cloud_service_account', '')
if isinstance(val, dict):
    val = json.dumps(val)
print(f\"export AI_SERVICE_GOOGLE_CLOUD_SERVICE_ACCOUNT='{val}'\")
")"

export INTERNAL_SERVICE_AUTHENTICATION_INTERNAL_SERVICE_SECRET=dummy
export SESSION_MANAGER_SECRET=dummy
export EXTERNAL_API_KEY_AUTHENTICATION_API_KEYS="dummy:dummy"
export EXTERNAL_API_KEY_AUTHENTICATION_METADATA_HEADER="tsunade-api-key"

export TSUNADE_EXTERNAL_GRPC_DISABLE_TLS="true"
export TSUNADE_EXTERNAL_GRPC_SOCKET_PATH="/tmp/tsunade.socket"

export POSTGRES_HOST=localhost
export POSTGRES_PORT=32422
export POSTGRES_USER=postgres
export POSTGRES_DATABASE=postgres
export POSTGRES_PASSWORD=postgres

export AI_POSTGRES_HOST=${POSTGRES_HOST}
export AI_POSTGRES_PORT=${POSTGRES_PORT}
export AI_POSTGRES_USER=tsunade
export AI_POSTGRES_DATABASE=tsunade
export AI_POSTGRES_PASSWORD=tsunade

# Create temporary authentication files for local development.
LOCAL_AUTH_DIR=$(mktemp -d)
trap "rm -rf ${LOCAL_AUTH_DIR}" EXIT

cat > "${LOCAL_AUTH_DIR}/external_service_accounts.json" << 'EOF'
{
  "service_accounts": [
    {
      "id": "dummy",
      "type": "SERVICE_ACCOUNT_TYPE_API_KEY",
      "permissions": ["*"],
    }
  ]
}
EOF

cat > "${LOCAL_AUTH_DIR}/internal_service_accounts.json" << 'EOF'
{
  "service_accounts": [
    {
      "id": "ai-service",
      "type": "SERVICE_ACCOUNT_TYPE_INTERNAL_SERVICE",
      "role_ids": [],
    }
  ]
}
EOF

cat > "${LOCAL_AUTH_DIR}/permissions.json" << 'EOF'
local external_service_accounts = import 'external_service_accounts.json';
local internal_service_accounts = import 'internal_service_accounts.json';
{
  "service_accounts": internal_service_accounts.service_accounts + external_service_accounts.service_accounts,
  "roles": [
    {
      "id": "ai:text_to_text",
      "permissions": [
        "/malonaz.ai.ai_service.v1.AiService/TextToText",
        "/malonaz.ai.ai_service.v1.AiService/TextToTextStream",
      ],
    },
  ],
}
EOF

plz build //cmd/tsunade

./plz-out/bin/cmd/tsunade/tsunade \
    --logging.format=pretty \
    --prometheus.disable \
    --external-api-key-authentication.config=${LOCAL_AUTH_DIR}/external_service_accounts.json \
    --internal-service-authentication.config=${LOCAL_AUTH_DIR}/internal_service_accounts.json \
    --permission-authentication.config=${LOCAL_AUTH_DIR}/permissions.json

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
    'AI_SERVICE_TYPESAFE_API_KEY': 'typesafe_api_key',
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

export AUTHENTICATION_SERVER_SECRET=dummy
export AUTHENTICATION_API_KEYS="dummy:dummy"
export AUTHENTICATION_API_KEY_HEADER="tsunade-api-key"

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

# A local configuration: the local API key reaches every method.
LOCAL_AUTH_DIR=$(mktemp -d)
trap "rm -rf ${LOCAL_AUTH_DIR}" EXIT

cat > "${LOCAL_AUTH_DIR}/authentication.jsonnet" << 'EOF'
{
  service_accounts: [
    { id: 'dummy', grants: [{ role: 'all', scope: '*' }] },
    { id: 'ai-service' },
  ],
  roles: [{ id: 'all', rules: [{ methods: ['*'] }] }],
}
EOF

plz build //cmd/tsunade

./plz-out/bin/cmd/tsunade/tsunade \
    --logging.format=pretty \
    --prometheus.disable \
    --authentication.config=${LOCAL_AUTH_DIR}/authentication.jsonnet

#!/usr/bin/env bash
set -e

export INTERNAL_SERVICE_AUTHENTICATION_INTERNAL_SERVICE_SECRET=dummy
export SESSION_MANAGER_SECRET=dummy
export EXTERNAL_API_KEY_AUTHENTICATION_API_KEYS="sgpt:dummy"
export EXTERNAL_API_KEY_AUTHENTICATION_METADATA_HEADER="tsunade-api-key"

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

export TSUNADE_EXTERNAL_GRPC_DISABLE_TLS="true"
export TSUNADE_EXTERNAL_GRPC_SOCKET_PATH="/tmp/tsunade.socket"

plz build //cmd/tsunade

./plz-out/bin/cmd/tsunade/tsunade \
    --logging.format=pretty \
    --prometheus.disable \
    --external-api-key-authentication.config=cmd/tsunade/authentication/external_service_accounts.json \
    --internal-service-authentication.config=cmd/tsunade/authentication/internal_service_accounts.json \
    --permission-authentication.config=cmd/tsunade/authentication/roles.json

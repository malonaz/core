#!/bin/bash

# Script to call the TextToText API using gcurl

# Colors
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
RESET='\033[0m'

# Default values
SOCKET="/tmp/core.socket"
PROVIDER="openai"
MODEL="gpt-4o"
SYSTEM_MESSAGE="You are a helpful assistant."
USER_MESSAGE="Hello, how are you?"
MAX_TOKENS=1000
TEMPERATURE=1
REASONING_EFFORT=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --socket)
      SOCKET="$2"
      shift 2
      ;;
    --provider)
      PROVIDER="$2"
      shift 2
      ;;
    --model)
      MODEL="$2"
      shift 2
      ;;
    --system)
      SYSTEM_MESSAGE="$2"
      shift 2
      ;;
    --message)
      USER_MESSAGE="$2"
      shift 2
      ;;
    --max-tokens)
      MAX_TOKENS="$2"
      shift 2
      ;;
    --temperature)
      TEMPERATURE="$2"
      shift 2
      ;;
    --reasoning)
      REASONING_EFFORT="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --socket PATH        Unix socket path (default: /tmp/core.socket)"
      echo "  --provider PROVIDER  Provider name (default: openai)"
      echo "  --model MODEL        Model ID (default: gpt-4o)"
      echo "  --system MESSAGE     System message (default: 'You are a helpful assistant.')"
      echo "  --message MESSAGE    User message (default: 'Hello, how are you?')"
      echo "  --max-tokens N       Max tokens to generate (default: 1000)"
      echo "  --temperature N      Temperature 0.0-2.0 (default: 1.0)"
      echo "  --reasoning EFFORT   Reasoning effort: LOW, MEDIUM, HIGH (optional)"
      echo ""
      echo "Example providers:"
      echo "  openai, anthropic, google"
      echo ""
      echo "Example models:"
      echo "  OpenAI: gpt-4o, gpt-4-turbo, o1"
      echo "  Anthropic: claude-sonnet-3.7, claude-sonnet-4"
      echo "  Google: gemini-flash-2, gemini-flash-2.5"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Build the model resource name
MODEL_RESOURCE_NAME="providers/${PROVIDER}/models/${MODEL}"

# Build the configuration object
CONFIG_JSON="\"max_tokens\": $MAX_TOKENS, \"temperature\": $TEMPERATURE"

# Add reasoning_effort if specified
if [[ -n "$REASONING_EFFORT" ]]; then
  CONFIG_JSON="$CONFIG_JSON, \"reasoning_effort\": \"REASONING_EFFORT_$REASONING_EFFORT\""
fi

# Build the JSON request
REQUEST=$(cat <<EOF
{
  "model": "$MODEL_RESOURCE_NAME",
  "messages": [
    {
      "role": "ROLE_SYSTEM",
      "content": "$SYSTEM_MESSAGE"
    },
    {
      "role": "ROLE_USER",
      "content": "$USER_MESSAGE"
    }
  ],
  "configuration": {
    $CONFIG_JSON
  }
}
EOF
)

echo "┌─────────────────────────────────────────────────────────"
echo "│ Provider: $PROVIDER"
echo "│ Model: $MODEL"
echo "│ Message: $USER_MESSAGE"
if [[ -n "$REASONING_EFFORT" ]]; then
  echo "│ Reasoning: REASONING_EFFORT_$REASONING_EFFORT"
fi
echo "└─────────────────────────────────────────────────────────"
echo ""

# Make the API call and process the streaming response
gcurl -plaintext -unix -d "$REQUEST" "$SOCKET" malonaz.core.ai.ai_service.v1.Ai/TextToTextStream | \
jq -r --unbuffered '
  if .contentChunk then
    "CONTENT:" + .contentChunk
  elif .reasoningChunk then
    "REASONING:" + .reasoningChunk
  elif .modelUsage then
    "\n\n┌─────────────────────────────────────────────────────────\n│ MODEL USAGE\n├─────────────────────────────────────────────────────────\n│ Model: \(.modelUsage.model)\n│ Input tokens: \(.modelUsage.inputToken.quantity // 0)\n│ Output tokens: \(.modelUsage.outputToken.quantity // 0)\n│ Reasoning tokens: \(.modelUsage.outputReasoningToken.quantity // 0)\n│ Cache read tokens: \(.modelUsage.inputCacheReadToken.quantity // 0)\n│ Cache write tokens: \(.modelUsage.inputCacheWriteToken.quantity // 0)\n└─────────────────────────────────────────────────────────"
  elif .generationMetrics then
    "\n┌─────────────────────────────────────────────────────────\n│ GENERATION METRICS\n├─────────────────────────────────────────────────────────\n│ Time to first byte: \(.generationMetrics.ttfb)\n│ Time to last byte: \(.generationMetrics.ttlb)\n└─────────────────────────────────────────────────────────"
  else
    empty
  end
' | while IFS= read -r line; do
  if [[ "$line" == "┌"* ]] || [[ "$line" == "│"* ]] || [[ "$line" == "├"* ]] || [[ "$line" == "└"* ]]; then
    echo "$line"
  elif [[ -z "$line" ]]; then
    echo ""  # Print empty lines
  elif [[ "$line" == "REASONING:"* ]]; then
    printf "${YELLOW}%s${RESET}" "${line#REASONING:}"
  elif [[ "$line" == "CONTENT:"* ]]; then
    printf "${CYAN}%s${RESET}" "${line#CONTENT:}"
  else
    printf "%s" "$line"
  fi
done

echo "" # Final newline at the end

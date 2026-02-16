#!/usr/bin/env bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

HOST_PORT=""
NAME=""
POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      HOST_PORT="$2"
      shift 2
      ;;
    --name)
      NAME="$2"
      shift 2
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

ACTION="${POSITIONAL_ARGS[0]}"

if [[ -z "$NAME" ]]; then
  echo -e "${RED}Error: --name is required${NC}"
  echo "Usage: $0 {start|stop|reset} --name NAME --port PORT"
  exit 1
fi

if [[ -z "$HOST_PORT" ]]; then
  echo -e "${RED}Error: --port is required${NC}"
  echo "Usage: $0 {start|stop|reset} --name NAME --port PORT"
  exit 1
fi

CONTAINER_NAME="nats_${NAME}_${HOST_PORT}"

case "$ACTION" in
  start)
    echo -e "${BLUE}Starting NATS container '$CONTAINER_NAME' with JetStream...${NC}"
    docker run -d \
      --name $CONTAINER_NAME \
      -p $HOST_PORT:4222 \
      nats:latest \
      -js >/dev/null

    if [ $? -eq 0 ]; then
      echo -e "${GREEN}NATS '$CONTAINER_NAME' started successfully on port $HOST_PORT${NC}"
      echo -e "${YELLOW}Connection string:${NC} nats://localhost:$HOST_PORT"
    else
      echo -e "${RED}Failed to start NATS container '$CONTAINER_NAME'${NC}"
      exit 1
    fi
    ;;

  stop)
    echo -e "${BLUE}Stopping NATS container '$CONTAINER_NAME'...${NC}"
    docker stop $CONTAINER_NAME >/dev/null
    docker rm $CONTAINER_NAME >/dev/null
    echo -e "${GREEN}NATS container '$CONTAINER_NAME' stopped and removed${NC}"
    ;;

  reset)
    echo -e "${YELLOW}Resetting NATS container '$CONTAINER_NAME'...${NC}"
    $0 stop --name $NAME --port $HOST_PORT
    $0 start --name $NAME --port $HOST_PORT
    ;;

  *)
    echo "Usage: $0 {start|stop|reset} --name NAME --port PORT"
    exit 1
    ;;
esac

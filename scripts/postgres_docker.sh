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
  echo "Usage: $0 {start|stop|reset|connect [db_name]} --name NAME --port PORT"
  exit 1
fi

if [[ -z "$HOST_PORT" ]]; then
  echo -e "${RED}Error: --port is required${NC}"
  echo "Usage: $0 {start|stop|reset|connect [db_name]} --name NAME --port PORT"
  exit 1
fi

CONTAINER_NAME="postgres_${NAME}_${HOST_PORT}"
POSTGRES_PASSWORD="postgres"
POSTGRES_USER="postgres"
CONTAINER_PORT="5432"

case "$ACTION" in
  start)
    echo -e "${BLUE}Starting PostgreSQL container '$CONTAINER_NAME'...${NC}"
    docker run -d \
      --name $CONTAINER_NAME \
      -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
      -e POSTGRES_USER=$POSTGRES_USER \
      -e POSTGRES_DB=postgres \
      -p $HOST_PORT:$CONTAINER_PORT \
      postgres:latest >/dev/null

    if [ $? -eq 0 ]; then
      echo -e "${GREEN}PostgreSQL '$CONTAINER_NAME' started successfully on port $HOST_PORT${NC}"
      echo -e "${YELLOW}Connection string:${NC} postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$HOST_PORT/postgres"
    else
      echo -e "${RED}Failed to start PostgreSQL container '$CONTAINER_NAME'${NC}"
      exit 1
    fi
    ;;

  stop)
    echo -e "${BLUE}Stopping PostgreSQL container '$CONTAINER_NAME'...${NC}"
    docker stop $CONTAINER_NAME >/dev/null
    docker rm $CONTAINER_NAME >/dev/null
    echo -e "${GREEN}PostgreSQL container '$CONTAINER_NAME' stopped and removed${NC}"
    ;;

  reset)
    echo -e "${YELLOW}Resetting PostgreSQL container '$CONTAINER_NAME'...${NC}"
    $0 stop --name $NAME --port $HOST_PORT
    $0 start --name $NAME --port $HOST_PORT
    ;;

  connect)
    DB_NAME="${POSITIONAL_ARGS[1]:-postgres}"
    echo -e "${BLUE}Connecting to PostgreSQL database '$DB_NAME'...${NC}"
    PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -p $HOST_PORT -U $POSTGRES_USER -d "$DB_NAME"
    ;;

  *)
    echo "Usage: $0 {start|stop|reset|connect [db_name]} --name NAME --port PORT"
    exit 1
    ;;
esac

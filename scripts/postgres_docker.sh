#!/usr/bin/env bash

HOST_PORT="54399"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      HOST_PORT="$2"
      shift 2
      ;;
    *)
      ACTION="$1"
      shift
      ;;
  esac
done

CONTAINER_NAME="postgres_dev_${HOST_PORT}"
POSTGRES_PASSWORD="postgres"
POSTGRES_USER="postgres"
POSTGRES_DB="postgres"
CONTAINER_PORT="5432"

case "$ACTION" in
  start)
    echo "Starting PostgreSQL container..."
    docker run -d \
      --name $CONTAINER_NAME \
      -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
      -e POSTGRES_USER=$POSTGRES_USER \
      -e POSTGRES_DB=$POSTGRES_DB \
      -p $HOST_PORT:$CONTAINER_PORT \
      postgres:latest

    if [ $? -eq 0 ]; then
      echo "PostgreSQL started successfully on port $HOST_PORT"
      echo "Connection string: postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$HOST_PORT/$POSTGRES_DB"
    else
      echo "Failed to start PostgreSQL container"
      exit 1
    fi
    ;;

  stop)
    echo "Stopping PostgreSQL container..."
    docker stop $CONTAINER_NAME
    docker rm $CONTAINER_NAME
    echo "PostgreSQL container stopped and removed"
    ;;

  reset)
    echo "Resetting PostgreSQL container..."
    $0 stop --port $HOST_PORT
    $0 start --port $HOST_PORT
    ;;

  connect)
    echo "Connecting to PostgreSQL database: $POSTGRES_DB..."
    PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -p $HOST_PORT -U $POSTGRES_USER -d $POSTGRES_DB
    ;;

  *)
    echo "Usage: $0 {start|stop|reset|connect} [--port PORT]"
    exit 1
    ;;
esac

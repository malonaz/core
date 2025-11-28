#!/bin/bash
export $(cat .env | grep -v '^#' | xargs)
plz run //server:local

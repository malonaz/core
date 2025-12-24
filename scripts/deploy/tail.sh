#!/bin/bash
set -e

ssh h-malonaz "journalctl -u ai-service -f"

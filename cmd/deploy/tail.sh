#!/bin/bash
set -e

ssh h-malonaz "journalctl -u sgpt -f"
